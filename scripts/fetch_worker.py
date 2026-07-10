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

ARTICLES_PER_COMPANY = 15
MIN_DELAY_SEC        = 8
MAX_DELAY_SEC        = 15
MAX_RETRIES          = 3
CIRCUIT_BLOCK_LIMIT  = 5
CIRCUIT_WINDOW_SEC   = 180
CIRCUIT_COOLDOWN_SEC = 900
REQUEST_TIMEOUT      = 20
LOOKBACK_WINDOW      = "1d"   # Google News time filter: 1d, 7d, 1m, etc.

IST = timezone(timedelta(hours=5, minutes=30))

# Only valid curl_cffi 0.15.0+ profiles.
# Generic aliases ("chrome", "safari") always resolve to latest supported.
IMPERSONATE_PROFILES = [
    "chrome",
    "chrome131",
    "chrome124",
    "chrome120",
    "safari",
    "edge101",
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
    """
    Returns a tuple (status, articles):
      status = "ok"      -> got articles
      status = "empty"   -> Google returned valid RSS with no items (no news)
      status = "blocked" -> Google throttled us (all retries exhausted)
      status = "error"   -> network / TLS / other error
    """
    if not company_name or not isinstance(company_name, str):
        return ("error", [])

    q = urlq(company_name.strip(), safe="")
    url = (f"https://news.google.com/rss/search?"
           f"q={q}+when:{LOOKBACK_WINDOW}"
           f"&hl=en-IN&gl=IN&ceid=IN:en")

    last_status = "error"

    for attempt in range(MAX_RETRIES):
        try:
            profile = random.choice(IMPERSONATE_PROFILES)

            # Try the randomly chosen profile; if it's unsupported in the
            # installed curl_cffi version, silently fall back to "chrome".
            try:
                r = cffi.get(
                    url,
                    headers=build_headers(),
                    impersonate=profile,
                    timeout=REQUEST_TIMEOUT,
                )
            except Exception as e:
                if "impersonate" in type(e).__name__.lower():
                    r = cffi.get(
                        url,
                        headers=build_headers(),
                        impersonate="chrome",
                        timeout=REQUEST_TIMEOUT,
                    )
                else:
                    raise

            body = r.text or ""

            # --- Case 1: blocked (real 429 or stealth block inside 200) ---
            if r.status_code == 429 or is_block_page(body):
                circuit.hit()
                try:
                    wait = int(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                except Exception:
                    wait = 2 ** (attempt + 1)
                wait += random.uniform(2, 5)
                log(f"  Blocked '{company_name[:30]}' "
                    f"(try {attempt + 1}) -> sleep {wait:.1f}s")
                last_status = "blocked"
                time.sleep(wait)
                continue

            # --- Case 2: valid RSS response ---
            if r.status_code == 200 and body.lstrip().startswith("<?xml"):
                articles = parse_rss(body.encode("utf-8"))
                if articles:
                    return ("ok", articles)
                else:
                    # Genuine "no news" — do not retry, do not mark as failure
                    return ("empty", [])

            # --- Case 3: other HTTP error ---
            last_status = "error"
            time.sleep((2 ** attempt) + random.uniform(1, 3))

        except Exception as e:
            log(f"  ! Error '{company_name[:30]}': {type(e).__name__}")
            last_status = "error"
            time.sleep((2 ** attempt) + random.uniform(1, 3))

    return (last_status, [])


def main():
    log(f"Starting - chunk {CHUNK_INDEX + 1} of {TOTAL_CHUNKS}")

    df = pd.read_excel(COMPANY_FILE)

    # Auto-detect company-name column
    col = None
    for c in df.columns:
        cl = str(c).lower()
        if "company" in cl or "name" in cl:
            col = c
            break
    if col is None:
        col = df.columns[0]

    all_companies = [
        str(x).strip()
        for x in df[col].dropna().unique()
        if str(x).strip() and str(x).strip().lower() != "nan"
    ]

    # Modulo slicing => no overlap, no gaps, works for any list size
    my_slice = [
        c for i, c in enumerate(all_companies)
        if i % TOTAL_CHUNKS == CHUNK_INDEX
    ]
    log(f"Assigned {len(my_slice)} companies out of {len(all_companies)}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["CompanyName", "NewsDescription", "Source",
             "PublishDate", "ArticleLink"]
        )

        success = 0
        empty   = 0
        blocked = 0
        errors  = 0

        for i, company in enumerate(my_slice, 1):
            # Human-like randomized delay
            time.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))

            status, articles = fetch_company(company)

            if status == "ok":
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
            elif status == "empty":
                empty += 1
            elif status == "blocked":
                blocked += 1
            else:
                errors += 1

            if i % 25 == 0:
                log(f"  Progress {i}/{len(my_slice)} "
                    f"(ok:{success} empty:{empty} "
                    f"blocked:{blocked} err:{errors})")

    log(f"Done. ok={success} empty={empty} blocked={blocked} "
        f"errors={errors} total={len(my_slice)}, Output={OUT_FILE}")


if __name__ == "__main__":
    main()
