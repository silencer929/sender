#!/usr/bin/env python3
"""
gmail_bulk_mailer.py

Send single or bulk emails via Gmail SMTP (smtp.gmail.com:587 TLS).
Supports HTML templates (saved as .html or .txt) or inline templates passed on the command line.
Template variables should be written using double curly braces, e.g. {{first_name}}.
This avoids conflicts with CSS/JS blocks that use single curly braces.

Usage examples (single):
  python gmail_bulk_mailer.py --to user@example.com --subject "Hello {{first_name}}" \
    --template "<h1>Hi {{first_name}}</h1><p>Your plan: {{plan}}</p>" --vars first_name=John --vars plan=Gold

Bulk CSV example:
  python gmail_bulk_mailer.py --csv contacts.csv --template template.html --subject "Hi {{first_name}}" \
    --password-env MAIL_PASSWORD --out send_log.csv --delay 0.3
"""
from __future__ import annotations
import argparse
import csv
import getpass
import os
import re
import sys
import time
import string
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import smtplib

# ---------- Defaults (from your provided config; can be overridden by ENV or CLI) ----------
DEFAULT_MAIL_HOST = "smtp.gmail.com"
DEFAULT_MAIL_PORT = 587
DEFAULT_MAIL_USER = "kenyasurveyhub@gmail.com"
DEFAULT_MAIL_PASS = 'gzyt pdia vrzx gbuz'
DEFAULT_FROM_ADDR = "kenyasurveyhub@gmail.com"
DEFAULT_FROM_NAME = "Kenya Survey Hub"
DEFAULT_ENCRYPTION ="tls"

# ---------------- Utility classes / functions ----------------

class SafeDict(dict):
    def __missing__(self, key):
        return ""

def load_template(template_arg: str) -> str:
    p = Path(template_arg)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return template_arg

def strip_html_tags(html: str) -> str:
    """Very small HTML->text fallback for plain-text alternative."""
    # Remove script/style
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
    # Replace breaks and paragraphs with newlines
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</p\s*>", "\n\n", html)
    # Remove all tags
    text = re.sub(r"<[^>]+>", "", html)
    # Unescape basic entities
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    # Collapse multiple newlines/spaces
    text = re.sub(r"\n\s+\n", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()

def build_message(template: str, row: Dict[str, Any]) -> str:
    """
    Format template safely while:
      - Treating CSS/JS single-brace blocks as literal (no formatting).
      - Allowing variables written as double-curly like {{first_name}} to be used as placeholders.
    Approach:
      1. Find all double-curly placeholders {{name}} and replace with temporary markers.
      2. Escape all remaining single braces so CSS/JS becomes literal.
      3. Replace markers with single-brace placeholders {name}.
      4. Apply .format_map() with a SafeDict of values (including _upper/_lower).
    """
    # Prepare values
    clean = {k: ("" if v is None else str(v)) for k, v in row.items()}
    # add upper/lower variants
    for k, v in list(clean.items()):
        try:
            clean[f"{k}_upper"] = v.upper()
        except Exception:
            clean[f"{k}_upper"] = str(v)
        try:
            clean[f"{k}_lower"] = v.lower()
        except Exception:
            clean[f"{k}_lower"] = str(v)

    # 1) extract double-curly placeholders like {{ first_name }} -> marker
    pattern = re.compile(r'\{\{\s*([A-Za-z0-9_]+)\s*\}\}')
    markers: Dict[str, str] = {}
    marker_prefix = "__TMPL_VAR_"
    marker_counter = 0

    def _marker_repl(m: re.Match) -> str:
        nonlocal marker_counter
        name = m.group(1)
        marker = f"{marker_prefix}{marker_counter}__"
        markers[marker] = name
        marker_counter += 1
        return marker

    tmp = pattern.sub(_marker_repl, template)

    # 2) escape any remaining single braces so CSS/JS blocks are preserved literally
    tmp = tmp.replace("{", "{{").replace("}", "}}")

    # 3) restore the markers as single-brace placeholders {name}
    for marker, name in markers.items():
        tmp = tmp.replace(marker, "{" + name + "}")

    # 4) format with SafeDict
    return tmp.format_map(SafeDict(clean))

def read_csv_headers(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    return headers

def iter_rows_with_headers(path: Path) -> Iterable[Dict[str, str]]:
    """Yield dicts for each data row using the header row as keys.
       Also guarantees canonical key 'email' mapped from first column if header not present."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        for raw_row in reader:
            # pad short rows
            if len(raw_row) < len(headers):
                raw_row += [""] * (len(headers) - len(raw_row))
            row = {headers[i]: raw_row[i] for i in range(len(headers))}
            if "email" not in {h.lower() for h in headers}:
                if len(raw_row) >= 1 and "email" not in row:
                    row.setdefault("email", raw_row[0])
            yield row

# ---------------- Email sending ----------------

def connect_smtp(host: str, port: int, username: str, password: str, use_tls: bool = True, timeout: float = 60.0) -> smtplib.SMTP:
    smtp = smtplib.SMTP(host, port, timeout=timeout)
    smtp.ehlo()
    if use_tls:
        smtp.starttls()
        smtp.ehlo()
    if username:
        smtp.login(username, password)
    return smtp

def send_email_message(smtp: smtplib.SMTP, from_addr: str, from_name: str, to_addr: str, subject: str, html_body: str, text_body: Optional[str] = None) -> None:
    msg = EmailMessage()
    msg["From"] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    if not text_body:
        text_body = strip_html_tags(html_body)
    msg.set_content(text_body)
    # add html alternative
    msg.add_alternative(html_body, subtype="html")
    smtp.send_message(msg)

# ---------------- CLI / Main ----------------

def parse_args():
    p = argparse.ArgumentParser(description="Send email(s) via Gmail SMTP using HTML templates (file or inline). Use {{var}} placeholders.")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--to", help="Recipient email address (single send).")
    target.add_argument("--csv", "-c", help="CSV file for bulk send (first row headers; must include 'email' or first column will be used).")
    p.add_argument("--template", "-t", required=True,
                   help="Template path (.html or .txt) OR inline template string. Use placeholders like {{first_name}}.")
    p.add_argument("--subject", "-s", required=False, default="No Subject",
                   help="Email subject (may include placeholders, use {{var}}).")
    p.add_argument("--username", help="SMTP username (default from env or config).", default=DEFAULT_MAIL_USER)
    p.add_argument("--password", help="SMTP password. If omitted will use env MAIL_PASSWORD or prompt.", default=None)
    p.add_argument("--password-env", help="Read password from environment variable name (e.g. MAIL_PASSWORD). If set, overrides --password.", default=None)
    p.add_argument("--from-address", help="From address (default from config).", default=DEFAULT_FROM_ADDR)
    p.add_argument("--from-name", help="From name (default from config).", default=DEFAULT_FROM_NAME)
    p.add_argument("--host", help="SMTP host (default smtp.gmail.com).", default=DEFAULT_MAIL_HOST)
    p.add_argument("--port", help="SMTP port (default 587).", type=int, default=DEFAULT_MAIL_PORT)
    p.add_argument("--no-tls", help="Do not use STARTTLS (not recommended).", action="store_true")
    p.add_argument("--vars", action="append", help="Template variables for single send: key=value (repeatable).")
    p.add_argument("--delay", type=float, default=0.2, help="Delay seconds between sends in bulk (default 0.2).")
    p.add_argument("--retries", "-r", type=int, default=2, help="Retries on failure for each email (default 2).")
    p.add_argument("--timeout", type=float, default=60.0, help="SMTP connection timeout in seconds.")
    p.add_argument("--dry-run", action="store_true", help="Prepare messages but do not send network requests.")
    p.add_argument("--out", "-o", default="email_send_log.csv", help="Output CSV log path (default email_send_log.csv).")
    p.add_argument("--limit", type=int, default=0, help="If >0, only process first N rows (for testing).")
    p.add_argument("--start-row", type=int, default=1, help="1-indexed data row to start from (useful to resume).")
    return p.parse_args()

def parse_vars_list(vars_list: Optional[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not vars_list:
        return out
    for item in vars_list:
        if "=" in item:
            k, v = item.split("=", 1)
            out[k.strip()] = v
    return out

def main():
    args = parse_args()
    template_raw = load_template(args.template)
    smtp_password = None
    if args.password_env:
        smtp_password = os.getenv(args.password_env, "")
    if smtp_password == "" and args.password:
        smtp_password = args.password
    if smtp_password == "":
        smtp_password = DEFAULT_MAIL_PASS
    if smtp_password == "":
        # prompt securely
        try:
            smtp_password = getpass.getpass(prompt=f"Password for {args.username}: ")
        except Exception:
            smtp_password = ""
    if smtp_password is None:
        smtp_password = ""

    # prepare output CSV
    out_fields = []
    total_processed = 0
    total_sent = 0

    # single-send mode
    if args.to:
        vars_map = parse_vars_list(args.vars)
        # Build row for formatting
        row = {k: v for k, v in vars_map.items()}
        # also include common placeholders
        row.setdefault("to", args.to)
        subject = build_message(args.subject, row)
        html_body = build_message(template_raw, row)
        text_body = strip_html_tags(html_body)
        print(f"Prepared message -> To: {args.to} | Subject: {subject}")
        if args.dry_run:
            print("[DRY RUN] Not sending. Message preview (first 500 chars):\n")
            print(html_body[:2000] + ("..." if len(html_body) > 500 else ""))
            return
        # send
        try:
            smtp = connect_smtp(args.host, args.port, args.username, DEFAULT_MAIL_PASS, use_tls=not args.no_tls, timeout=args.timeout)
            send_email_message(smtp, args.from_address, args.from_name, args.to, subject, html_body, text_body)
            smtp.quit()
            print("Email sent successfully.")
        except Exception as e:
            print(f"Failed to send email: {e}", file=sys.stderr)
            sys.exit(1)
        return

    # bulk CSV mode
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(2)
    headers = read_csv_headers(csv_path)
    out_fields = list(headers) + ["email_final", "subject", "attempts", "success", "error", "sent_at"]
    with open(args.out, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=out_fields)
        writer.writeheader()
        session_smtp = None
        smtp_connected = False
        for idx, row in enumerate(iter_rows_with_headers(csv_path), start=1):
            if idx < args.start_row:
                continue
            if args.limit and total_processed >= args.limit:
                break
            total_processed += 1
            to_addr = row.get("email", "") or row.get("Email", "") or ""
            if not to_addr:
                # skip if no recipient
                log_row = {h: row.get(h, "") for h in headers}
                log_row.update({"email_final": "", "subject": "", "attempts": 0, "success": False, "error": "no-email", "sent_at": ""})
                writer.writerow(log_row)
                continue
            subject = build_message(args.subject, row)
            html_body = build_message(template_raw, row)
            text_body = strip_html_tags(html_body)
            log_row = {h: row.get(h, "") for h in headers}
            log_row.update({"email_final": to_addr, "subject": subject, "attempts": 0, "success": False, "error": "", "sent_at": ""})
            if args.dry_run:
                print(f"[DRY] Row {idx} -> To: {to_addr} | Subject: {subject}")
                writer.writerow(log_row)
                total_sent += 1
                continue
            # connect SMTP lazily (connect once and reuse)
            if not smtp_connected:
                try:
                    session_smtp = connect_smtp(args.host, args.port, args.username, DEFAULT_MAIL_PASS, use_tls=not args.no_tls, timeout=args.timeout)
                    smtp_connected = True
                except Exception as e:
                    log_row["error"] = f"smtp_connect_failed: {e}"
                    writer.writerow(log_row)
                    print(f"Failed to connect to SMTP: {e}", file=sys.stderr)
                    break
            attempt = 0
            success = False
            last_err = ""
            while attempt <= args.retries:
                attempt += 1
                log_row["attempts"] = attempt
                try:
                    send_email_message(session_smtp, args.from_address, args.from_name, to_addr, subject, html_body, text_body)
                    success = True
                    break
                except Exception as e:
                    last_err = str(e)
                    # reconnect on some failures
                    try:
                        session_smtp.quit()
                    except Exception:
                        pass
                    time.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
                    try:
                        session_smtp = connect_smtp(args.host, args.port, args.username, smtp_password, use_tls=not args.no_tls, timeout=args.timeout)
                    except Exception as e2:
                        last_err = f"{last_err}; reconnect_failed:{e2}"
                        # proceed to next retry which will attempt reconnect again
            log_row["success"] = success
            log_row["error"] = "" if success else last_err
            log_row["sent_at"] = datetime.utcnow().isoformat() + "Z" if success else ""
            writer.writerow(log_row)
            total_sent += 1
            # polite delay
            time.sleep(args.delay)
        # close smtp if open
        if session_smtp:
            try:
                session_smtp.quit()
            except Exception:
                pass
    print(f"Processed {total_processed} rows. Log written to {args.out}")

if __name__ == "__main__":
    main()
