#!/usr/bin/env python3
"""
traccar_bulk_sms.py

Reads a CSV (first row = headers; first col = first name, second = last name, third = phone)
and sends personalized SMS via a Traccar-compatible local phone gateway.

Usage examples:
  python traccar_bulk_sms.py --csv contacts.csv --template template.txt --gateway http://192.168.1.23:8082 --auth "Token" --dry-run
  python traccar_bulk_sms.py -c contacts.csv -t "Hi {first_name}, your plan {plan} expires {expiry_date}" -g http://192.168.1.23:8082
"""
from __future__ import annotations
import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional
import requests

# ----------------- Helpers -----------------

class SafeDict(dict):
    def __missing__(self, key):
        return ""

def normalize_phone(phone: Optional[str], country_prefix: Optional[str]=None, ensure_plus: bool=False) -> str:
    if phone is None:
        return ""
    s = str(phone).strip()
    for ch in (" ", "-", "(", ")", ".", "\u200e", "\u200f"):
        s = s.replace(ch, "")
    if s == "":
        return ""
    # if starts with 00 -> convert to +
    if s.startswith("00"):
        s = "+" + s[2:]
    # optionally prepend country prefix when no leading + and number looks local (starts with 0 or digit)
    if not s.startswith("+") and country_prefix:
        # if it already starts with the country prefix digits, don't duplicate
        cp_digits = country_prefix.lstrip("+")
        if not s.startswith(cp_digits):
            # strip leading zeros if present then add prefix
            s = s.lstrip("0")
            s = country_prefix + s
    if ensure_plus and not s.startswith("+"):
        s = "+" + s
    return s

def build_message(template: str, row: Dict[str, Any]) -> str:
    # convert all values to strings and provide upper/lower variants
    clean = {k: ("" if v is None else str(v)) for k, v in row.items()}
    # add safe derived keys
    for k, v in list(clean.items()):
        clean[f"{k}_upper"] = v.upper()
        clean[f"{k}_lower"] = v.lower()
    return template.format_map(SafeDict(clean))

def send_to_gateway(session: requests.Session, url: str, to: str, message: str, auth: Optional[str], timeout: float
                   ) -> Tuple[bool, Optional[int], str]:
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = auth
    payload = {"to": to, "message": message}
    try:
        resp = session.post(url, json=payload, headers=headers, timeout=timeout)
        text = resp.text if resp is not None else ""
        return (resp.ok if resp is not None else False, resp.status_code if resp is not None else None, text)
    except requests.RequestException as e:
        return (False, None, str(e))

# ----------------- CSV reading utilities -----------------

def read_csv_headers(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    return headers

def iter_rows_with_headers(path: Path) -> Iterable[Dict[str, str]]:
    """Yield dicts for each data row using the header row as keys.
       Also guarantees canonical keys: first_name, last_name, phone mapped from first 3 columns if not present."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        for raw_row in reader:
            # pad short rows
            if len(raw_row) < len(headers):
                raw_row += [""] * (len(headers) - len(raw_row))
            row = {headers[i]: raw_row[i] for i in range(len(headers))}
            # canonical mapping from positions if canonical not present
            if "first_name" not in {h.lower() for h in headers}:
                # set canonical keys regardless of header names, but don't overwrite existing keys
                if len(raw_row) >= 1 and "first_name" not in row:
                    row.setdefault("first_name", raw_row[0])
            if "last_name" not in {h.lower() for h in headers}:
                if len(raw_row) >= 2 and "last_name" not in row:
                    row.setdefault("last_name", raw_row[1])
            if "phone" not in {h.lower() for h in headers}:
                if len(raw_row) >= 3 and "phone" not in row:
                    row.setdefault("phone", raw_row[2])
            yield row

# ----------------- Main -----------------

def parse_args():
    p = argparse.ArgumentParser(description="Bulk SMS sender via Traccar-compatible phone gateway.")
    p.add_argument("--csv", "-c", required=True, help="Input CSV file path (first row headers).")
    p.add_argument("--template", "-t", required=True,
                   help="Template string or path to template file. Use placeholders like {first_name}, {plan}.")
    p.add_argument("--gateway", "-g", required=True, help="Gateway URL, e.g. http://192.168.1.23:8082")
    p.add_argument("--auth", "-a", default=None, help="Authorization token value (optional).")
    p.add_argument("--delay", "-d", type=float, default=0.2, help="Delay seconds between sends (default 0.2).")
    p.add_argument("--retries", "-r", type=int, default=2, help="Retries on failure (default 2).")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout seconds (default 10).")
    p.add_argument("--dry-run", action="store_true", help="Prepare messages but do not send network requests.")
    p.add_argument("--start-row", type=int, default=1, help="1-indexed data row to start from (useful to resume).")
    p.add_argument("--limit", type=int, default=0, help="If >0, only process first N rows (for testing).")
    p.add_argument("--out", "-o", default="sms_send_log.csv", help="Output CSV log path (default sms_send_log.csv).")
    p.add_argument("--country-prefix", "-p", default=None, help="Optional country prefix to add when phone lacks + (e.g. +254).")
    p.add_argument("--ensure-plus", action="store_true", help="Ensure normalized phone begins with + by adding one if missing.")
    return p.parse_args()

def load_template(template_arg: str) -> str:
    p = Path(template_arg)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return template_arg

def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(2)
    template = load_template(args.template)
    headers = read_csv_headers(csv_path)
    # prepare output fields: original headers + meta
    out_fields = list(headers) + ["phone_normalized", "message", "attempts", "success", "status_code", "response", "sent_at"]
    session = requests.Session()
    total_processed = 0
    total_sent = 0
    with open(args.out, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=out_fields)
        writer.writeheader()
        for idx, row in enumerate(iter_rows_with_headers(csv_path), start=1):
            if idx < args.start_row:
                continue
            if args.limit and total_processed >= args.limit:
                break
            total_processed += 1
            # normalize phone
            raw_phone = row.get("phone", "") or row.get("Phone", "") or ""
            phone_norm = normalize_phone(raw_phone, country_prefix=args.country_prefix, ensure_plus=args.ensure_plus)
            # build message
            message = build_message(template, row)
            # prepare log row (preserve original header values where possible)
            log_row = {h: row.get(h, "") for h in headers}
            log_row.update({"phone_normalized": phone_norm, "message": message, "attempts": 0, "success": False, "status_code": "", "response": "", "sent_at": ""})
            if args.dry_run:
                print(f"[DRY] Row {idx} -> To: {phone_norm} | Message({len(message)}): {message[:160]}{'...' if len(message)>160 else ''}")
                writer.writerow(log_row)
                total_sent += 1
                continue
            # send with retries
            attempt = 0
            success = False
            last_status = None
            last_resp = ""
            while attempt <= args.retries:
                attempt += 1
                log_row["attempts"] = attempt
                ok, status_code, resp_text = send_to_gateway(session, args.gateway, phone_norm, message, auth=args.auth, timeout=args.timeout)
                last_status = status_code
                last_resp = resp_text
                if ok:
                    success = True
                    break
                # exponential backoff (capped)
                backoff = min(5.0, 0.5 * (2 ** (attempt - 1)))
                time.sleep(backoff)
            log_row["success"] = success
            log_row["status_code"] = "" if last_status is None else str(last_status)
            log_row["response"] = last_resp
            log_row["sent_at"] = datetime.utcnow().isoformat() + "Z"
            writer.writerow(log_row)
            total_sent += 1
            # polite delay between sends
            time.sleep(args.delay)
    print(f"Processed {total_processed} rows. Log written to {args.out}")

if __name__ == "__main__":
    main()
