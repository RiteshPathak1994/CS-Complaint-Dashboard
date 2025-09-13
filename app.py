# app.py - Professional Version (Dropbox for attachments, Drive removed)
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
from dotenv import load_dotenv
import io
import plotly.express as px
from streamlit_option_menu import option_menu
import calendar
import time
import json
import logging
import dropbox   # ✅ Dropbox for attachments

# ----------------------------
# CONFIG
# ----------------------------
IMPL_SPREADSHEET_NAME = "Master Implementation sheet"
IMPL_TAB_NAME = "CS Team"

COMPLAINT_SPREADSHEET_NAME = "CS Comp"
COMPLAINT_TAB_NAME = "Comp"

REPORTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1OJWpQOwevw1W5iNUk6dm_wfZphuBdjboCkrQwYwTNOY/edit#gid=0"

# ----------------------------
# Load local .env (for local dev)
# ----------------------------
load_dotenv()

# ----------------------------
# Logging for debugging
# ----------------------------
logger = logging.getLogger("cs_complaint_auth")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ----------------------------
# Helper utilities for secrets
# ----------------------------
def _fix_private_key_str(s):
    """Fix common private_key formatting issues (escaped \n)."""
    if not s or not isinstance(s, str):
        return s
    s = s.strip()
    # Remove surrounding triple-quotes if present
    if (s.startswith('"""') and s.endswith('"""')) or (s.startswith("'''") and s.endswith("'''")):
        s = s[3:-3]
    # replace escaped newlines with real newlines
    if "\\n" in s and "-----BEGIN" in s:
        s = s.replace("\\n", "\n")
    return s

def _normalize_service_account_info(info):
    """
    Ensure info is a dict, and fix private_key format if present.
    """
    if not isinstance(info, dict):
        return info
    info = dict(info)  # shallow copy
    if "private_key" in info and info["private_key"]:
        info["private_key"] = _fix_private_key_str(info["private_key"])
    return info

def _secrets_table_to_info(table):
    """
    Convert TOML table from st.secrets (GCP_SERVICE_ACCOUNT) into a dict suitable
    for Credentials.from_service_account_info.
    """
    if not isinstance(table, dict):
        return table
    info = {}
    for k, v in table.items():
        info[k] = v
    # fix private_key formatting just in case
    return _normalize_service_account_info(info)

# ----------------------------
# Read admin & email config from secrets/env
# ----------------------------
def _secret_or_env(key, default=None):
    if isinstance(st.secrets, dict) and key in st.secrets:
        return st.secrets.get(key)
    return os.environ.get(key, default)

ADMIN_EMAIL = _secret_or_env("ADMIN_EMAIL", os.environ.get("ADMIN_EMAIL", "your_email@gmail.com"))
MANAGER_EMAIL = _secret_or_env("MANAGER_EMAIL", os.environ.get("MANAGER_EMAIL", "manager_email@gmail.com"))
EMAIL_USER = _secret_or_env("EMAIL_USER", os.environ.get("EMAIL_USER"))
EMAIL_PASS = _secret_or_env("EMAIL_PASS", os.environ.get("EMAIL_PASS"))

# ----------------------------
# AUTH functions (robust) for Google Sheets
# ----------------------------
def _resolve_service_account_info_from_secrets():
    """
    Try multiple sources and return a credentials-info dict if found, else None.
    Sources (in order):
      - st.secrets["GCP_SERVICE_ACCOUNT_JSON"] (string containing JSON)
      - st.secrets["GCP_SERVICE_ACCOUNT"] (TOML table)
      - env var GCP_SERVICE_ACCOUNT_JSON (string containing JSON)
      - env var GOOGLE_APPLICATION_CREDENTIALS (path) -> handled elsewhere
      - local file service_account.json -> handled elsewhere
    """
    # 1) st.secrets JSON string
    try:
        if isinstance(st.secrets, dict) and "GCP_SERVICE_ACCOUNT_JSON" in st.secrets:
            raw = st.secrets["GCP_SERVICE_ACCOUNT_JSON"]
            logger.info("Auth: using st.secrets['GCP_SERVICE_ACCOUNT_JSON']")
            info = json.loads(raw) if isinstance(raw, str) else raw
            return _normalize_service_account_info(info)
    except Exception as e:
        logger.warning(f"Auth (st.secrets JSON) parse failed: {e}")

    # 2) st.secrets TOML table
    try:
        if isinstance(st.secrets, dict) and "GCP_SERVICE_ACCOUNT" in st.secrets:
            table = st.secrets["GCP_SERVICE_ACCOUNT"]
            logger.info("Auth: using st.secrets['GCP_SERVICE_ACCOUNT'] table")
            info = _secrets_table_to_info(table)
            return info
    except Exception as e:
        logger.warning(f"Auth (st.secrets table) parse failed: {e}")

    # 3) env var JSON
    env_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    if env_json:
        try:
            logger.info("Auth: using environment GCP_SERVICE_ACCOUNT_JSON")
            info = json.loads(env_json)
            return _normalize_service_account_info(info)
        except Exception as e:
            logger.warning(f"Auth (env JSON) parse failed: {e}")

    return None

@st.cache_resource
def _create_creds_from_info(info, scopes):
    """Wrap Credentials.from_service_account_info with normalization."""
    info = _normalize_service_account_info(info)
    return Credentials.from_service_account_info(info, scopes=scopes)

def get_gspread_client_try():
    """
    Attempt to create a gspread client. Returns client if successful, else raises.
    """
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    # 1) Try st.secrets / env JSON path
    info = _resolve_service_account_info_from_secrets()
    if info:
        try:
            creds = _create_creds_from_info(info, scopes=scope)
            logger.info("Auth success using in-memory service account info")
            return gspread.authorize(creds)
        except Exception as e:
            logger.warning(f"Auth using in-memory info failed: {e}")

    # 2) GOOGLE_APPLICATION_CREDENTIALS path
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        try:
            if os.path.exists(creds_path):
                creds = Credentials.from_service_account_file(creds_path, scopes=scope)
                logger.info("Auth success using GOOGLE_APPLICATION_CREDENTIALS file")
                return gspread.authorize(creds)
            else:
                logger.warning(f"GOOGLE_APPLICATION_CREDENTIALS set but file not found: {creds_path}")
        except Exception as e:
            logger.warning(f"Auth (GOOGLE_APPLICATION_CREDENTIALS) failed: {e}")

    # 3) local file
    local_path = "service_account.json"
    if os.path.exists(local_path):
        try:
            creds = Credentials.from_service_account_file(local_path, scopes=scope)
            logger.info("Auth success using local service_account.json")
            return gspread.authorize(creds)
        except Exception as e:
            logger.warning(f"Auth (local service_account.json) failed: {e}")

    # If nothing worked, raise with guidance
    raise RuntimeError(
        "Google service account credentials not found. Provide credentials by one of the following:\n"
        "1) On Streamlit Cloud: Manage app → Secrets → add GCP_SERVICE_ACCOUNT (table) OR GCP_SERVICE_ACCOUNT_JSON (full JSON string).\n"
        "   - For table-style, keys must match the JSON key names (type, project_id, private_key_id, private_key, client_email, client_id, auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url, etc.).\n"
        "2) Set environment variable GCP_SERVICE_ACCOUNT_JSON to the full JSON content.\n"
        "3) Set GOOGLE_APPLICATION_CREDENTIALS to the path to a key file on the server.\n"
        "4) Place service_account.json in the app folder (not recommended for public repos).\n"
    )

# Try to create client now but handle failures gracefully so app doesn't crash at import
try:
    client = get_gspread_client_try()
    logger.info("gspread client created")
except Exception as e:
    client = None
    logger.error(f"Could not create gspread client: {e}")

# ----------------------------
# Dropbox Upload
# ----------------------------
def _get_dropbox_client_from_secrets():
    """
    Create a Dropbox client. Prefer refresh-token flow (long lived) if provided:
      - DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET
    Fallback to DROPBOX_ACCESS_TOKEN (dev/test only).
    """
    # Prefer st.secrets then environment
    secrets = st.secrets if isinstance(st.secrets, dict) else {}
    refresh_token = secrets.get("DROPBOX_REFRESH_TOKEN") or os.environ.get("DROPBOX_REFRESH_TOKEN")
    app_key = secrets.get("DROPBOX_APP_KEY") or os.environ.get("DROPBOX_APP_KEY")
    app_secret = secrets.get("DROPBOX_APP_SECRET") or os.environ.get("DROPBOX_APP_SECRET")
    access_token = secrets.get("DROPBOX_ACCESS_TOKEN") or os.environ.get("DROPBOX_ACCESS_TOKEN")

    if refresh_token and app_key and app_secret:
        # Use refresh-token flow (recommended)
        logger.info("Dropbox: using refresh-token auth from secrets/env")
        return dropbox.Dropbox(oauth2_refresh_token=refresh_token, app_key=app_key, app_secret=app_secret)
    elif access_token:
        logger.info("Dropbox: using plain access token (dev/test mode)")
        return dropbox.Dropbox(access_token)
    else:
        raise RuntimeError("Dropbox credentials not found in st.secrets or environment. Provide DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET (recommended) or DROPBOX_ACCESS_TOKEN for testing.")

def upload_to_dropbox(file):
    """Upload file to Dropbox and return a shareable link (direct download)."""
    if not file:
        return ""
    try:
        dbx = _get_dropbox_client_from_secrets()

        # ensure filename is safe
        filename = file.name
        dropbox_path = f"/{filename}"

        content = file.getvalue()
        dbx.files_upload(content, dropbox_path, mode=dropbox.files.WriteMode("overwrite"))

        try:
            res = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            link = res.url
        except dropbox.exceptions.ApiError:
            links_res = dbx.sharing_list_shared_links(path=dropbox_path, direct_only=True)
            links = links_res.links
            if links:
                link = links[0].url
            else:
                raise

        if link.endswith("?dl=0"):
            link = link.replace("?dl=0", "?dl=1")
        elif "?dl=1" not in link:
            link = link + "?dl=1"

        return link
    except Exception as e:
        logger.error(f"Dropbox upload failed: {e}")
        raise RuntimeError(f"Dropbox upload failed: {e}")

# ----------------------------
# EMAIL HELPER
# ----------------------------
def send_email_with_attachments(to_emails, subject, body, attachments=None):
    """
    to_emails: list or comma-separated string
    attachments: list of dicts: {"filename": "report.csv", "content": bytes, "mimetype": "text/csv"}
    """
    if isinstance(to_emails, str):
        to_emails = [e.strip() for e in to_emails.split(",") if e.strip()]
    sender_email = EMAIL_USER or os.getenv("EMAIL_USER")
    sender_password = EMAIL_PASS or os.getenv("EMAIL_PASS")
    if not sender_email or not sender_password:
        st.error("Email credentials not set (EMAIL_USER / EMAIL_PASS). Cannot send email.")
        return False, "Email credentials not configured"
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # attach files
    if attachments:
        for att in attachments:
            filename = att.get("filename", "attachment")
            content = att.get("content", b"")
            part = MIMEBase("application", "octet-stream")
            part.set_payload(content)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_emails, msg.as_string())
        server.quit()
        return True, None
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False, str(e)

def get_reporter_email(reporter_name):
    if client is None:
        logger.warning("get_reporter_email: gspread client not available")
        return None
    try:
        reporter_ss = client.open_by_url(REPORTER_SHEET_URL)
        reporter_ws = reporter_ss.get_worksheet(0)
        records = reporter_ws.get_all_records()
        for row in records:
            if str(row.get("Reporter Name", "")).strip().lower() == reporter_name.strip().lower():
                return row.get("Email Address")
    except Exception as e:
        logger.warning(f"get_reporter_email failed: {e}")
        return None
    return None

# ----------------------------
# HELPERS (data)
# ----------------------------
@st.cache_data(ttl=300)
def load_school_names():
    if client is None:
        st.warning("Google credentials not configured. School list cannot be loaded.")
        return []
    impl_ss = client.open(IMPL_SPREADSHEET_NAME)
    impl_ws = impl_ss.worksheet(IMPL_TAB_NAME)
    values = impl_ws.get_all_values()
    if not values:
        return []
    header = [h.strip() for h in values[0]]
    rows = values[1:]
    school_col_idx = next((i for i, h in enumerate(header) if "school" in h.lower()), 0)
    schools = [r[school_col_idx].strip() for r in rows if len(r) > school_col_idx and r[school_col_idx].strip()]
    return list(dict.fromkeys(schools))  # unique

def _find_header_index(headers, candidates):
    """
    Return index in headers for the first candidate that matches (case-insensitive contains).
    headers: list of header strings
    candidates: list of variants e.g. ["File link", "Attachment Link"]
    returns index or None
    """
    if not headers:
        return None
    lower_headers = [str(h).lower() for h in headers]
    for cand in candidates:
        cand_l = cand.lower()
        for i, h in enumerate(lower_headers):
            # exact match or contains either way
            if cand_l == h or cand_l in h or h in cand_l:
                return i
    return None

def append_complaint_row_dict(row_dict):
    """
    Append a complaint row to the spreadsheet by matching header names.
    row_dict: dict mapping logical field names to values. Expected logical names:
      - timestamp, school, reporter, category, severity, product, description, status, tat, file_link
    The sheet's header row is inspected; values are placed into matching columns.
    Any headers not found are left blank. If sheet has extra columns, those are preserved.
    If sheet is shorter than mapping, extra values are appended at the end.

    This implementation also *forces*:
      - Status into the 8th column (index 7) if possible (ensuring "Open" when missing).
      - File link into the 11th column (index 10) if no attachment header is found.
    """
    if client is None:
        raise RuntimeError("Google credentials not configured. Cannot append complaint.")
    comp_ss = client.open(COMPLAINT_SPREADSHEET_NAME)
    comp_ws = comp_ss.worksheet(COMPLAINT_TAB_NAME)

    # read header row
    headers = comp_ws.row_values(1)
    num_cols = max(1, len(headers))

    # prepare output row with blanks
    out_row = [""] * num_cols

    # mapping of logical names -> list of header candidate strings (common variants)
    mapping = {
        "timestamp": ["timestamp", "time", "created", "created at"],
        "school": ["school", "institution", "organisation", "organization"],
        "reporter": ["reporter", "reporter name", "name"],
        "category": ["category", "issue", "type"],
        "severity": ["priority", "severity"],
        "product": ["product", "prod"],
        "description": ["description", "details", "issue description", "comment"],
        "status": ["status"],
        "tat": ["tat", "turnaround time", "turnaround", "tat (hrs)"],
        "file_link": ["file link", "attachment link", "attachment", "file", "attachment_url", "attachment link (direct)", "attachment link"]
    }

    # helper to set value into out_row at header match index
    def set_for_logical(key, value):
        nonlocal out_row, headers
        candidates = mapping.get(key, [key])
        idx = _find_header_index(headers, candidates)
        if idx is not None:
            # if we need to extend out_row
            if idx >= len(out_row):
                out_row.extend([""] * (idx - len(out_row) + 1))
            out_row[idx] = value
            return True
        return False

    # Use lowercased keys for matching convenience
    val_map = {}
    for k, v in row_dict.items():
        val_map[k.lower()] = v

    # set each logical field
    set_for_logical("timestamp", val_map.get("timestamp") or val_map.get("time") or val_map.get("created") or "")
    set_for_logical("school", val_map.get("school") or "")
    set_for_logical("reporter", val_map.get("reporter") or val_map.get("reporter name") or "")
    set_for_logical("category", val_map.get("category") or val_map.get("issue") or "")
    set_for_logical("severity", val_map.get("severity") or val_map.get("priority") or "")
    set_for_logical("product", val_map.get("product") or "")
    set_for_logical("description", val_map.get("description") or val_map.get("details") or "")
    # If a status was provided use it, otherwise default to Open
    provided_status = val_map.get("status") or "Open"
    # try to set using header match (this sets status if header exists)
    status_set = set_for_logical("status", provided_status)

    # --- NEW: ensure we place status into the actual status header index if found ---
    status_idx = _find_header_index(headers, mapping["status"])
    if status_idx is not None:
        # ensure out_row long enough
        if status_idx >= len(out_row):
            out_row.extend([""] * (status_idx - len(out_row) + 1))
        out_row[status_idx] = provided_status
    else:
        # ensure status appears in 8th column (index 7) as a fallback
        desired_status_index = 7  # position 8 (1-based)
        if len(out_row) <= desired_status_index:
            out_row.extend([""] * (desired_status_index - len(out_row) + 1))
        if not out_row[desired_status_index]:
            out_row[desired_status_index] = provided_status

    # TAT may be intentionally empty; set if provided
    set_for_logical("tat", val_map.get("tat") or "")

    # file link should go to 'File link' or 'Attachment Link' if present; otherwise place into 11th column (index 10)
    file_val = val_map.get("file_link") or val_map.get("attachment") or val_map.get("attachment link") or ""
    file_set = set_for_logical("file_link", file_val)

    # --- NEW: ensure we place file link into actual attachment header index if found ---
    file_idx = _find_header_index(headers, mapping["file_link"])
    if file_idx is not None:
        if file_idx >= len(out_row):
            out_row.extend([""] * (file_idx - len(out_row) + 1))
        out_row[file_idx] = file_val
    else:
        # fallback to position 11 (index 10)
        desired_file_index = 10  # position 11 (1-based)
        if len(out_row) <= desired_file_index:
            out_row.extend([""] * (desired_file_index - len(out_row) + 1))
        out_row[desired_file_index] = file_val

    # If timestamp header didn't exist, ensure timestamp at start (index 0)
    ts_idx = _find_header_index(headers, mapping["timestamp"])
    if ts_idx is None:
        ts_val = val_map.get("timestamp") or get_ist_timestamp()
        # DON'T insert at front (that shifts indices). Instead set index 0 safely.
        if len(out_row) == 0:
            out_row = [ts_val]
        else:
            # ensure out_row has at least 1 element and set index 0
            if len(out_row) < 1:
                out_row.extend([""] * (1 - len(out_row)))
            # If index 0 already has something (rare), leave it only if it looks like a timestamp; otherwise overwrite.
            try_overwrite = False
            if not out_row[0]:
                try_overwrite = True
            else:
                # crude heuristic: if existing value looks nothing like a timestamp, overwrite it with ts_val
                existing = str(out_row[0])
                if len(existing.strip()) < 4 or not any(ch.isdigit() for ch in existing):
                    try_overwrite = True
            if try_overwrite:
                out_row[0] = ts_val
            # otherwise keep whatever's at index 0 (avoid shifting)

    # Final safety: ensure out_row length is at least the header count (so append_row uses same number of cols)
    if len(out_row) < num_cols:
        out_row.extend([""] * (num_cols - len(out_row)))

    # Debug: log header vs row lengths and a preview of out_row for troubleshooting
    logger.info(f"append_complaint_row_dict: headers ({len(headers)}): {headers}")
    logger.info(f"append_complaint_row_dict: final out_row length {len(out_row)}; preview: {out_row[:min(50, len(out_row))]}")

    # Append the row to the sheet
    comp_ws.append_row(out_row)

def append_complaint_row(row):
    """
    Backwards-compatible helper: accepts a list or dict.
    If list -> append directly (legacy).
    If dict -> use append_complaint_row_dict to place values into appropriate columns.
    """
    if isinstance(row, dict):
        append_complaint_row_dict(row)
    elif isinstance(row, (list, tuple)):
        # legacy: append list directly
        if client is None:
            raise RuntimeError("Google credentials not configured. Cannot append complaint.")
        comp_ss = client.open(COMPLAINT_SPREADSHEET_NAME)
        comp_ws = comp_ss.worksheet(COMPLAINT_TAB_NAME)
        comp_ws.append_row(list(row))
    else:
        raise ValueError("append_complaint_row expects a list or dict")

def get_ist_timestamp():
    tz = pytz.timezone("Asia/Kolkata")
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

@st.cache_data(ttl=120)
def fetch_recent_complaints(limit=5000):
    if client is None:
        st.warning("Google credentials not configured. Cannot fetch complaints.")
        return pd.DataFrame()
    comp_ss = client.open(COMPLAINT_SPREADSHEET_NAME)
    comp_ws = comp_ss.worksheet(COMPLAINT_TAB_NAME)
    records = comp_ws.get_all_records()
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).tail(limit)

# ----------------------------
# MONTHLY REPORT BUILDER
# ----------------------------
def parse_dates_series(series):
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)

def build_monthly_report(df_all, year:int, month:int):
    """
    df_all: DataFrame with complaints (raw)
    year, month: ints
    returns: dict with report summary, csv bytes for attachment
    """
    report = {}
    # defensive column names
    ts_col = find_col(df_all, ["timestamp", "time", "created"])
    status_col = find_col(df_all, ["status"])
    cat_col = find_col(df_all, ["category", "issue", "type"])
    school_col = find_col(df_all, ["school", "institution", "organisation", "organization"])
    severity_col = find_col(df_all, ["priority", "severity"])
    closed_ts_col = find_col(df_all, ["closed", "resolved", "closed timestamp", "resolved timestamp", "closed_at", "resolved_at"])
    product_col = find_col(df_all, ["product"])  # <-- ADDED to include product if present

    # Parse timestamp column
    if ts_col:
        df_all["_ts"] = parse_dates_series(df_all[ts_col])
    else:
        df_all["_ts"] = pd.NaT

    # filter by requested month
    start = pd.Timestamp(year=year, month=month, day=1)
    last_day = calendar.monthrange(year, month)[1]
    end = pd.Timestamp(year=year, month=month, day=last_day) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    df_month = df_all[(df_all["_ts"].notna()) & (df_all["_ts"] >= start) & (df_all["_ts"] <= end)].copy()

    report['year'] = year
    report['month'] = month
    report['total'] = int(len(df_month))

    # status breakdown
    if status_col:
        df_month["_status_norm"] = df_month[status_col].astype(str).str.strip().str.lower()
        def label_status(s):
            s = str(s).strip().lower()
            if "open" in s:
                return "Open"
            if "progress" in s or "in progress" in s:
                return "In Progress"
            if "close" in s or "closed" in s or "resolved" in s:
                return "Closed"
            return s.title() if s else "Unknown"
        status_counts = df_month["_status_norm"].apply(label_status).value_counts().to_dict()
    else:
        status_counts = {}

    report['status_counts'] = {k: int(v) for k,v in status_counts.items()}

    # top categories
    if cat_col and not df_month.empty:
        top_cats = df_month[cat_col].astype(str).str.strip().replace("", "Unknown").value_counts().head(5).to_dict()
    else:
        top_cats = {}
    report['top_categories'] = {k: int(v) for k,v in top_cats.items()}

    # top schools
    if school_col and not df_month.empty:
        top_schools = df_month[school_col].astype(str).str.strip().replace("", "Unknown").value_counts().head(5).to_dict()
    else:
        top_schools = {}
    report['top_schools'] = {k: int(v) for k,v in top_schools.items()}

    # severity counts
    if severity_col and not df_month.empty:
        sev = df_month[severity_col].astype(str).str.strip().str.title().replace("", "Unknown").value_counts().to_dict()
    else:
        sev = {}
    report['severity_counts'] = {k: int(v) for k,v in sev.items()}

    # average time to close (if closed timestamp present)
    avg_close_hours = None
    if closed_ts_col and not df_month.empty:
        df_month["_closed_ts"] = parse_dates_series(df_month[closed_ts_col])
        # only where both ts and closed exist and closed >= ts
        valid = df_month[(df_month["_ts"].notna()) & (df_month["_closed_ts"].notna())]
        if not valid.empty:
            diffs = (valid["_closed_ts"] - valid["_ts"]).dt.total_seconds() / 3600.0
            diffs = diffs[diffs >= 0]  # ignore negative
            if not diffs.empty:
                avg_close_hours = round(float(diffs.mean()), 2)
    report['avg_close_hours'] = avg_close_hours

    # prepare CSV attachment of monthly complaints
    csv_bytes = None
    if not df_month.empty:
        keep_cols = []
        for c in [ts_col, status_col, cat_col, school_col, severity_col, product_col, closed_ts_col]:
            if c:
                keep_cols.append(c)
        export_df = df_month.copy()
        if keep_cols:
            export_cols = [c for c in keep_cols if c in export_df.columns]
            export_df = export_df[export_cols + [c for c in export_df.columns if c not in export_cols]]
        csv_bytes = export_df.to_csv(index=False).encode("utf-8")

    return report, csv_bytes

def format_report_text(report):
    ym = f"{report['month']:02d}/{report['year']}"
    lines = [f"Monthly Complaint Report - {ym}", ""]
    lines.append(f"Total Complaints: {report.get('total', 0)}")
    sc = report.get('status_counts', {})
    if sc:
        lines.append("Status counts:")
        for k,v in sc.items():
            lines.append(f"  • {k}: {v}")
    tc = report.get('top_categories', {})
    if tc:
        lines.append("Top Categories:")
        for k,v in tc.items():
            lines.append(f"  • {k}: {v}")
    ts = report.get('top_schools', {})
    if ts:
        lines.append("Top Schools:")
        for k,v in ts.items():
            lines.append(f"  • {k}: {v}")
    sev = report.get('severity_counts', {})
    if sev:
        lines.append("Severity:")
        for k,v in sev.items():
            lines.append(f"  • {k}: {v}")
    avg_close = report.get('avg_close_hours')
    if avg_close is not None:
        lines.append(f"Average time to close: {avg_close} hours")
    lines.append("")
    lines.append("Regards,\nCS Dashboard")
    return "\n".join(lines)

# ----------------------------
# Utility: find a column name by keyword(s)
# ----------------------------
def find_col(df, keywords):
    """Return first column from df.columns that contains any of keywords (case-insensitive)."""
    if df is None or df.columns.empty:
        return None
    cols = list(df.columns)
    for kw in keywords:
        for c in cols:
            if kw.lower() in str(c).lower():
                return c
    return None

def find_cols(df, keywords_list):
    """Return list of matching columns for any of keywords_list (any match)."""
    res = []
    if df is None or df.columns.empty:
        return res
    cols = list(df.columns)
    for c in cols:
        lc = str(c).lower()
        for kw in keywords_list:
            if kw.lower() in lc:
                res.append(c)
                break
    return res

def is_completed_value(v):
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return any(x in s for x in ["complete", "completed", "done", "yes", "y"])

# ----------------------------
# PAGE NAVIGATION (CLASSY LIGHT NAVBAR)
# ----------------------------
st.set_page_config(page_title="CS App", page_icon="📝", layout="wide")

selected = option_menu(
    menu_title="",
    options=["Complaints Dashboard"],  # only show complaints for now
    icons=["exclamation-circle"],
    orientation="horizontal",
    styles={
        "container": {"padding": "6px", "background-color": "#f8fafc", "border-bottom": "1px solid #e6edf3"},
        "icon": {"color": "#0b5ed7", "font-size": "18px"},
        "nav-link": {"font-size": "15px", "text-align": "center", "margin": "0 6px", "color": "#0f172a"},
        "nav-link-selected": {"background-color": "#0b5ed7", "color": "white", "font-weight": "600"},
    }
)

# ----------------------------
# PAGE 1: COMPLAINT DASHBOARD
# ----------------------------
if selected == "Complaints Dashboard":
    st.title("📝 CS Complaint Dashboard")

    # If client is missing, show big banner with guidance
    if client is None:
        st.error(
            "Google service account credentials not found. The app can run but Google Sheets/Drive features are disabled.\n\n"
            "Provide credentials in one of these ways:\n"
            " • Streamlit Secrets: add `GCP_SERVICE_ACCOUNT` (table) or `GCP_SERVICE_ACCOUNT_JSON` (JSON string).\n"
            " • Environment: set `GCP_SERVICE_ACCOUNT_JSON` to the JSON content.\n"
            " • Set `GOOGLE_APPLICATION_CREDENTIALS` to a key file path or upload `service_account.json` to the app folder.\n\n"
            "Check Streamlit → Manage app → Secrets. See logs for more details."
        )

    st.sidebar.header("⚙️ Complaint Controls")
    if st.sidebar.button("🔄 Refresh complaints"):
        st.cache_data.clear()
        try:
            st.experimental_rerun()
        except Exception:
            params = dict(st.query_params) if hasattr(st, "query_params") else {}
            params["_refresh"] = str(int(time.time()))
            st.query_params = params

    # Month selector for monthly report (defaults to current month)
    today = pd.Timestamp.now(tz=pytz.timezone("Asia/Kolkata"))
    sel_year = st.sidebar.selectbox("Report Year", options=list(range(today.year-2, today.year+1)), index=2)
    months = [calendar.month_name[i] for i in range(1,13)]
    sel_month_idx = st.sidebar.selectbox("Report Month", options=list(range(1,13)), index=today.month-1)
    # Note: sel_month_idx is 1..12

    if st.sidebar.button("📧 Send Monthly Report"):
        df_all = fetch_recent_complaints(limit=5000)
        if df_all.empty:
            st.warning("No complaints data available to send report.")
        else:
            with st.spinner("Building monthly report..."):
                report, csv_bytes = build_monthly_report(df_all, sel_year, sel_month_idx)
                body = format_report_text(report)
                attachments = []
                if csv_bytes:
                    attachments.append({"filename": f"complaints_{sel_year}_{sel_month_idx:02d}.csv", "content": csv_bytes, "mimetype": "text/csv"})
                to_list = [ADMIN_EMAIL, MANAGER_EMAIL]
                ok, err = send_email_with_attachments(to_list, f"📊 Monthly Complaints Report - {sel_month_idx:02d}/{sel_year}", body, attachments=attachments)
                if ok:
                    st.success("✅ Monthly report sent to you and your manager!")
                else:
                    st.error(f"Failed to send monthly report: {err}")

    status_filter = st.sidebar.selectbox("Filter by Status", ["All", "Open", "In Progress", "Closed"], index=0)

    # KPI DASHBOARD
    df_all = fetch_recent_complaints(limit=5000)
    if df_all.empty:
        st.info("ℹ️ No complaints data available yet.")
    else:
        total_complaints = len(df_all)
        if "Status" in df_all.columns:
            open_count = df_all[df_all["Status"].astype(str).str.strip().str.lower() == "open"].shape[0]
        else:
            open_count = 0
        open_percent = round((open_count / total_complaints) * 100, 1) if total_complaints else 0
        if "Timestamp" in df_all.columns:
            df_all["Timestamp"] = pd.to_datetime(df_all["Timestamp"], errors="coerce")
            this_month = df_all[df_all["Timestamp"].dt.to_period("M") == pd.Timestamp.now().to_period("M")]
        else:
            df_all["Timestamp"] = pd.Series(pd.to_datetime([], errors="coerce"))
            this_month = pd.DataFrame()
        top_category = this_month["Category"].value_counts().idxmax() if ("Category" in df_all.columns and not this_month.empty) else "N/A"
        c1, c2, c3 = st.columns(3)
        c1.metric("📌 Total Complaints", total_complaints)
        c2.metric("🟠 Open %", f"{open_percent}%")
        c3.metric("🏆 Top Category", top_category)

    st.markdown("---")

    # (the rest of the UI / form is the same as your original code)
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 1rem; border-radius: 12px; margin: 1rem 0;">
        <h3 style="color: white; text-align: center; margin: 0; font-weight: 600;">
            📝 Submit New Complaint
        </h3>
        <p style="color: rgba(255,255,255,0.9); text-align: center; margin: 0.5rem 0 0 0; font-size: 0.9rem;">
            Please provide detailed information to help us resolve your concern efficiently
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Main form container with styling (same as your original code)
    with st.container():
        st.markdown("""
        <style>
        .complaint-form {
            background: white;
            border-radius: 16px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            border: 1px solid #e5e7eb;
            padding: 2rem;
            margin: 1rem 0;
        }
        .form-section {
            margin-bottom: 1.5rem;
        }
        .section-header {
            color: #374151;
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #e5e7eb;
        }
        .required-field {
            color: #dc2626;
            font-weight: bold;
            font-size: 0.8rem;
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown('<div class="complaint-form">', unsafe_allow_html=True)

        with st.form("complaint_form", clear_on_submit=True):
            st.markdown('<div class="section-header">👤 Reporter Information</div>', unsafe_allow_html=True)
            col1, col2 = st.columns([1, 1], gap="large")

            with col1:
                reporter = st.text_input("Reporter", placeholder="Enter your name", help="This will be used for correspondence regarding your complaint", key="reporter_name")
                st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)

            with col2:
                schools = load_school_names()
                if schools:
                    school = st.selectbox("School/Institution", schools, index=0, help="Select your school from the dropdown", key="reporter_school")
                else:
                    school = st.text_input("School/Institution", placeholder="Enter your school name", help="Please type your school name manually", key="reporter_school_manual")
                st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)

            st.markdown('<div style="margin: 1.5rem 0;"></div>', unsafe_allow_html=True)

            st.markdown('<div class="section-header">📋 Complaint Details</div>', unsafe_allow_html=True)
            col3, col4 = st.columns([1, 1], gap="large")
            with col3:
                category = st.selectbox("Issue Category", ["Login Issue", "Access Issue", "Content Issue", "Technical", "Other"], help="Select the category that best describes your issue", key="complaint_category")
                st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)
            with col4:
                severity = st.selectbox("Priority Level", ["High", "Medium", "Low"], index=1, help="High: Urgent issue affecting work\nMedium: Important but not urgent\nLow: Minor issue or suggestion", key="complaint_severity")
                st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)

            st.markdown('<div style="margin: 1rem 0;"></div>', unsafe_allow_html=True)

            # <-- ADDED: Product dropdown
            st.markdown('<div class="section-header">🔖 Product</div>', unsafe_allow_html=True)
            product = st.selectbox("Product", ["BELLS", "HB", "PBL"], index=0, help="Select the product related to this complaint", key="complaint_product")
            st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)

            st.markdown('<div class="section-header">📝 Issue Description</div>', unsafe_allow_html=True)
            description = st.text_area("Detailed Description", height=120, placeholder="Please provide a clear and detailed description of the issue you are experiencing.", help="The more details you provide, the better we can assist you", key="complaint_description")
            st.markdown('<span class="required-field">*Required</span>', unsafe_allow_html=True)

            st.markdown('<div style="margin: 1.5rem 0;"></div>', unsafe_allow_html=True)

            st.markdown('<div class="section-header">📎 Supporting Documents</div>', unsafe_allow_html=True)
            uploaded_file = st.file_uploader("Upload Supporting Files (Optional)", type=["pdf", "jpg", "jpeg", "png", "docx", "xlsx"], help="You can attach screenshots, documents, or other files that help explain your issue", key="complaint_attachment")

            st.markdown('<div style="margin: 2rem 0 1rem 0;"></div>', unsafe_allow_html=True)

            col_submit = st.columns([1, 2, 1])
            with col_submit[1]:
                submitted = st.form_submit_button("🚀 Submit Complaint", use_container_width=True, type="primary")

        st.markdown('</div>', unsafe_allow_html=True)

        if submitted:
            # <-- MODIFIED: include Product in required fields
            required_fields = {"Reporter Name": reporter, "School": school, "Category": category, "Severity": severity, "Product": product, "Description": description}
            missing = [k for k, v in required_fields.items() if not str(v).strip()]
            if missing:
                st.error(f"⚠️ Please complete the following required fields: **{', '.join(missing)}**")
            else:
                try:
                    with st.spinner("Processing your complaint..."):
                        file_link = upload_to_dropbox(uploaded_file) if uploaded_file else ""
                        # Build a dict and let append_complaint_row_dict place fields into correct columns,
                        # ensuring the file link goes into "File link" / "Attachment Link" / "Attachment" if present,
                        # and guaranteeing Status in 8th column and File link in 11th column if headers missing.
                        row_dict = {
                            "timestamp": get_ist_timestamp(),
                            "school": school,
                            "reporter": reporter,
                            "category": category,
                            "severity": severity,
                            "product": product,
                            "description": description,
                            "status": "Open",
                            "tat": "",  # intentionally blank; sheet TAT column (if present) will remain blank
                            "file_link": file_link
                        }
                        append_complaint_row(row_dict)
                    st.success("✅ **Complaint Submitted Successfully!**")
                    st.info("📧 You will receive a confirmation email shortly with your complaint details and reference number.")
                    reporter_email = get_reporter_email(reporter)
                    if reporter_email:
                        body = f"""Dear {reporter},

Your complaint has been registered successfully.

Complaint Details:
• School: {school}
• Product: {product}
• Category: {category}
• Priority: {severity}
• Description: {description}
• Attachment: {file_link if file_link else 'None'}

We will review your complaint and get back to you soon.

Best regards,
Customer Success Team
"""
                        send_email_with_attachments(reporter_email, "Complaint Registered - Confirmation", body)
                        send_email_with_attachments(ADMIN_EMAIL, f"New Complaint Submitted by {reporter}", body)
                except Exception as e:
                    st.error(f"❌ **Submission Failed:** We encountered an error while processing your complaint. Please try again or contact support directly.\n\nTechnical details: {str(e)}")

    st.markdown('<div style="margin: 2rem 0;"></div>', unsafe_allow_html=True)
    st.markdown("---")

    # Recent complaints
    st.subheader("📊 Recent complaints")
    try:
        df_recent = fetch_recent_complaints(limit=50)
        if df_recent.empty:
            st.info("ℹ️ No complaints found yet.")
        else:
            if status_filter != "All" and "Status" in df_recent.columns:
                df_recent = df_recent[df_recent["Status"].astype(str).str.strip().str.lower() == status_filter.lower()]
            if df_recent.empty:
                st.info(f"ℹ️ No complaints with status '{status_filter}'.")
            else:
                # Prefer 'File link', but accept 'Attachment Link' or 'Attachment'
                display_link_col = None
                for candidate in ["File link", "File Link", "Attachment Link", "Attachment", "Attachment Link"]:
                    if candidate in df_recent.columns:
                        display_link_col = candidate
                        break
                # if attachment column present normalize and render clickable
                if display_link_col:
                    df_recent[display_link_col] = df_recent[display_link_col].apply(lambda x: f"[📎 View File]({x})" if x and str(x).strip() else "")
                if "Status" in df_recent.columns:
                    color_map = {"open": "🔴 Open", "in progress": "🟠 In Progress", "closed": "🟢 Closed"}
                    df_recent["Status"] = df_recent["Status"].apply(lambda x: color_map.get(str(x).strip().lower(), x))
                st.dataframe(df_recent, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"❌ Could not load recent complaints. Details: {e}")

# Implementation Monitoring (hidden)
if False:
    st.title("📈 Implementation Monitoring")
    df_impl = load_school_names()  # placeholder; original had load_implementation_data
    if df_impl is None or len(df_impl) == 0:
        st.info("ℹ️ No implementation data available.")
    else:
        st.info("Implementation Monitoring is currently hidden. Re-enable when ready.")
