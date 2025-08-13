import os
import csv
import time
import re
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
load_dotenv()  # Loads KEEPA_API_KEY from .env

try:
    import keepa
except ModuleNotFoundError:
    raise SystemExit("Run: pip install -r requirements.txt")

KEEPA_KEY = os.getenv("KEEPA_API_KEY")
if not KEEPA_KEY:
    raise SystemExit("Set KEEPA_API_KEY in your .env file")

INPUT_FILE = "seller_ids.txt"
OUTPUT_FILE = "gb_sellers_in_de.csv"

def extract_country_from_seller(seller_obj: Dict[str, Any]) -> Optional[str]:
    # Prefer explicit fields if present
    for k in ["establishedCountry", "countryCode", "country"]:
        v = seller_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()

    candidate_keys = [
        "businessAddress", "registrationAddress", "address",
        "storefrontAddress", "sellerAddress"
    ]

    for k in candidate_keys:
        v = seller_obj.get(k)
        if isinstance(v, list) and v:
            last = str(v[-1]).strip()
            if re.fullmatch(r"[A-Za-z]{2}", last):
                return last.upper()
            if re.search(r"(United\s+Kingdom|Great\s+Britain|\bUK\b|\bGB\b)", last, re.I):
                return "GB"
        elif isinstance(v, str) and v.strip():
            tail = v.strip().split(",")[-1]
            if re.fullmatch(r"\s*[A-Za-z]{2}\s*", tail):
                return tail.strip().upper()
            if re.search(r"(United\s+Kingdom|Great\s+Britain|\bUK\b|\bGB\b)", v, re.I):
                return "GB"

    extra = seller_obj.get("extra")
    if isinstance(extra, dict):
        for k in ["country", "countryCode", "establishedCountry"]:
            v = extra.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()
    return None

def amazon_seller_url(seller_id: str, domain: str = "DE") -> str:
    tld = {"DE": "de", "GB": "co.uk", "US": "com"}.get(domain, "de")
    return f"https://www.amazon.{tld}/sp?seller={seller_id}"

def keepa_seller_url(seller_id: str, domain: str = "DE") -> str:
    return f"https://keepa.com/#!seller/{domain}/{seller_id}"

def main():
    # 1) Read seller IDs
    if not os.path.exists(INPUT_FILE):
        raise SystemExit(f"Missing {INPUT_FILE}. Create it with one seller ID per line.")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        seller_ids = [line.strip() for line in f if line.strip()]

    if not seller_ids:
        raise SystemExit(f"{INPUT_FILE} has no seller IDs. Add at least one.")

    # 2) Init Keepa
    api = keepa.Keepa(KEEPA_KEY)

    rows: List[Dict[str, str]] = []
    BATCH = 100  # Keepa supports up to 100 seller IDs per call

    for i in range(0, len(seller_ids), BATCH):
        batch = seller_ids[i:i+BATCH]
        try:
            data = api.seller_query(batch, domain="DE", storefront=False, wait=True)
            for sid in batch:
                s = data.get(sid)
                if not isinstance(s, dict):
                    continue
                country = extract_country_from_seller(s)
                if country in {"GB", "UK"}:
                    rows.append({
                        "sellerId": sid,
                        "sellerName": str(s.get("sellerName") or "").strip(),
                        "establishedCountry": country,
                        "amazonUrl": amazon_seller_url(sid, "DE"),
                        "keepaUrl": keepa_seller_url(sid, "DE"),
                    })
        except Exception as e:
            print(f"[warn] batch {i//BATCH+1} failed: {e}")
            time.sleep(2)

    # 3) Write CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "sellerId", "sellerName", "establishedCountry", "amazonUrl", "keepaUrl"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Done. Wrote {len(rows)} GB/UK sellers to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

