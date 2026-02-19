# GEMINI.md - Project Overview and Usage Guide

## Project Overview

This project provides a suite of Python scripts for sending bulk communications, including emails and SMS messages. The scripts are designed to be run from the command line and support templating to personalize messages.

-   `email-sender.py`: A robust script for sending bulk emails using a Gmail account. It supports HTML templates, CSV contact lists, and features like adjustable delays, retries, and detailed logging.

-   `bsms.py`: A script for sending bulk SMS messages via a Traccar-compatible SMS gateway. It also uses CSV files for contact lists and supports message templating.

The `data` directory is intended to hold contact lists in CSV format, while the `emails` and `sms` directories can be used to store message templates.

## Building and Running

The scripts are written in Python 3 and can be run directly from the command line. No special build process is required.

### `email-sender.py`

To send a single email:

```bash
python email-sender.py --to user@example.com --subject "Hello {{first_name}}" --template "<h1>Hi {{first_name}}</h1>" --vars first_name=John
```

To send bulk emails using a CSV file:

```bash
python email-sender.py --csv data/contacts.csv --template emails/template.html --subject "Your Subject Here" --out email_send_log.csv
```

**Note:** The CSV file should have a header row. The script will look for an `email` column, or use the first column if it's not found.

### `bsms.py`

To send bulk SMS messages:

```bash
python bsms.py --csv data/sms_contacts.csv --template sms/template.txt --gateway <your_traccar_gateway_url> --auth "Your_Auth_Token"
```

**Note:** The CSV file for `bsms.py` is expected to have the phone number in the third column if a `phone` header is not present.

## Development Conventions

-   **Command-line Interface**: Both scripts use the `argparse` library for command-line argument parsing. To add new options, modify the `parse_args` function in the respective script.
-   **Templating**: The scripts use a custom double curly brace `{{variable}}` syntax for templating. This is to avoid conflicts with CSS/JS in HTML templates.
-   **Dependencies**: The scripts have minimal external dependencies. `email-sender.py` uses only standard Python libraries. `bsms.py` uses the `requests` library.
-   **Logging**: Both scripts generate a CSV log file (`email_send_log.csv` and `sms_send_log.csv` by default) to record the status of each message sent.
