"""
Per-chunk Google News RSS fetcher.
Reads CompanyList.xlsx, processes only its assigned chunk,
writes output/chunk_<idx>.csv.
"""
import os
import time
import random
import csv
from datetime import timezone, timedelta
from urllib.parse import quote as urlq
from xml.etree import ElementTree as ET

import pandas as pd
from curl_cffi import requests as cffi

# ─── CONFIG ────────────────────────────────────────────────────
CHUNK_INDEX  = int(os.environ.get("CHUNK_INDEX", "0"))
TOTAL_CHUNKS = int(os.environ.get("TOTAL_CHUNKS", "10"))

COMPANY_FILE = "data/CompanyList.xlsx"
OUT_DIR      = "output"
OUT_FILE     = f"{OUT_DIR}/chunk_{CHUNK_INDEX}.csv"

ARTICLES_PER_COMPANY = 10
MIN_DELAY_SEC        = 8
MAX_DELAY_SEC        = 15
MAX_RETRIES          = 3
CIRCUIT_BLOCK_LIMIT  = 5
CIRCUIT_WINDOW_SEC   = 180
CIRCUIT_COOLDOWN_SEC = 900
REQUEST_TIMEOUT      = 20

IST = timezone(timedelta(hours=5, minutes=30))

IMPERSONATE_PROFILES = [
    "chrome124", "chrome123", "chrome120",
    "safari17_2", "safari17_0", "edge101",
]
ACCEPT_LANGS = [
    "en-IN,en-US;q=0.9,en;q=0.8",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.9",
]

os.makedirs(OUT_DIR, exist_ok=True)


def log(msg):
    print(f"[chunk {CHUNK_INDEX}] {msg}", flush=True)


def is_block_page(text):
    if not text:
        return True
    t = text.lower()
    if "unusual traffic" in t or "/sorry/index" in t or "captcha" in t:
        return True
    if "<rss" not in t and "<feed" not in t and "<html" in t:
        return True
    return False


def build_headers():
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Referer": "https://news.google.com/",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def parse_rss(xml_bytes):
    out = []
    try:
        root = ET.fromstring(xml_bytes)
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            src_el = item.find("source")
            source = (src_el.text or "").strip() if src_el is not None else ""
            if title and link:
                out.append({
                    "title": title,
                    "link": link,
                    "source": source,
                    "pubDate": pub,
                })
    except ET.ParseError as e:
        log(f"  ! XML parse error: {e}")
    return out


class Circuit:
    def __init__(self):
        self.blocks = []

    def hit(self):
        now = time.time()
        self.blocks = [t for t in self.blocks if now - t < CIRCUIT_WINDOW_SEC]
        self.blocks.append(now)
        if len(self.blocks) >= CIRCUIT_BLOCK_LIMIT:
            log(f"  Circuit breaker: {CIRCUIT_BLOCK_LIMIT} blocks in "
                f"{CIRCUIT_WINDOW_SEC}s. Cooling down {CIRCUIT_COOLDOWN_SEC}s...")
            time.sleep(CIRCUIT_COOLDOWN_SEC)
            self.blocks.clear()


circuit = Circuit()


def fetch_company(company_name):
    if not company_name or not isinstance(company_name, str):
        return []
    q = urlq(company_name.strip(), safe="")
    url = (f"https://news.google.com/rss/search?"
           f"q={q}+when:1d&hl=en-IN&gl=IN&ceid=IN:en")

    for attempt in range(MAX_RETRIES):
        try:
            profile = random.choice(IMPERSONATE_PROFILES)
            r = cffi.get(
                url,
                headers=build_headers(),
                impersonate=profile,
                timeout=REQUEST_TIMEOUT,
            )
            body = r.text or ""

            if r.status_code == 429 or is_block_page(body):
                circuit.hit()
                try:
                    wait = int(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                except Exception:
                    wait = 2 ** (attempt + 1)
                wait += random.uniform(2, 5)
                log(f"  Block for '{company_name[:30]}' (try {attempt+1}) "
                    f"-> waiting {wait:.1f}s")
                time.sleep(wait)
                continue

            if r.status_code == 200 and body.lstrip().startswith("<?xml"):
                return parse_rss(body.encode("utf-8"))

            time.sleep((2 ** attempt) + random.uniform(1, 3))
        except Exception as e:
            log(f"  ! Error '{company_name[:30]}': {type(e).__name__}")
            time.sleep((2 ** attempt) + random.uniform(1, 3))

    return []


def main():
    log(f"Starting - chunk {CHUNK_INDEX + 1} of {TOTAL_CHUNKS}")

    df = pd.read_excel(COMPANY_FILE)

    col = None
    for c in df.columns:
        cl = str(c).lower()
        if "company" in cl or "name" in cl:
            col = c
            break
    if col is None:
        col = df.columns[0]

    all_companies = [str(x).strip() for x in df[col].dropna().unique()
                     if str(x).strip() and str(x).strip().lower() != "nan"]

    my_slice = [c for i, c in enumerate(all_companies)
                if i % TOTAL_CHUNKS == CHUNK_INDEX]
    log(f"Assigned {len(my_slice)} companies out of {len(all_companies)}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["CompanyName", "NewsDescription", "Source",
                         "PublishDate", "ArticleLink"])

        success = 0
        fail = 0
        for i, company in enumerate(my_slice, 1):
            time.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))

            articles = fetch_company(company)
            if articles:
                success += 1
                for art in articles[:ARTICLES_PER_COMPANY]:
                    writer.writerow([
                        company,
                        art["title"],
                        art["source"],
                        art["pubDate"],
                        art["link"],
                    ])
                f.flush()
            else:
                fail += 1

            if i % 25 == 0:
                log(f"  Progress {i}/{len(my_slice)} (ok:{success} fail:{fail})")

    log(f"Done. Success={success}, Fail={fail}, Output={OUT_FILE}")


if __name__ == "__main__":
    main()
