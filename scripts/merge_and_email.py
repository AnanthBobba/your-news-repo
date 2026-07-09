"""
Merges all chunk CSVs, produces the final Excel, emails it via Gmail.
"""
import os
import glob
import smtplib
import sys
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))
TODAY = datetime.now(IST).strftime("%d-%m-%Y")

OUT_XLSX = f"CompanyNews_{TODAY}.xlsx"

GMAIL_FROM         = os.environ["GMAIL_FROM"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_TO           = os.environ["EMAIL_TO"]


def log(m):
    print(m, flush=True)


files = sorted(glob.glob("chunks/**/chunk_*.csv", recursive=True))
log(f"Found {len(files)} chunk files")

if not files:
    log("No chunk files found. Exiting without email.")
    sys.exit(1)

dfs = []
for fp in files:
    try:
        df = pd.read_csv(fp)
        if not df.empty:
            dfs.append(df)
            log(f"  {fp}: {len(df)} rows")
    except Exception as e:
        log(f"  ! Skip {fp}: {e}")

if not dfs:
    log("All chunks empty.")
    sys.exit(1)

full = pd.concat(dfs, ignore_index=True)

before = len(full)
full = full.drop_duplicates(
    subset=["CompanyName", "NewsDescription", "ArticleLink"],
    keep="first",
)
log(f"Deduped {before} -> {len(full)} rows")

full = full.sort_values(
    by=["CompanyName", "PublishDate"],
    ascending=[True, False],
)

with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
    full.to_excel(w, sheet_name="NewsTable", index=False)

log(f"Excel written: {OUT_XLSX} ({len(full)} rows)")

n_companies = full["CompanyName"].nunique()

msg = EmailMessage()
msg["Subject"] = (
    f"Daily Company News - {TODAY} "
    f"({len(full)} articles, {n_companies} companies)"
)
msg["From"] = GMAIL_FROM
msg["To"] = EMAIL_TO
msg.set_content(
    f"Attached: today's Google News RSS report.\n\n"
    f"- Date: {TODAY}\n"
    f"- Total articles: {len(full):,}\n"
    f"- Companies with news: {n_companies:,}\n\n"
    f"- Automated report (GitHub Actions)"
)

with open(OUT_XLSX, "rb") as f:
    msg.add_attachment(
        f.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=OUT_XLSX,
    )

with smtplib.SMTP("smtp.gmail.com", 587) as s:
    s.starttls()
    s.login(GMAIL_FROM, GMAIL_APP_PASSWORD)
    s.send_message(msg)

log(f"Email sent to {EMAIL_TO}")
