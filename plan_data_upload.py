"""
Extract data from pune_data.xlsx sheet "HP" and generate CSV in Frappe RSU Master import template format.
Output matches RSU Master.csv template for Data Import (ERPNext v15).
Only includes clusters that exist in Site doctype (custom_lob=FTTH SDU, custom_type=RSU Cluster).
"""

import csv
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# File paths
SCRIPT_DIR = Path(__file__).parent
SOURCE_XLSX = SCRIPT_DIR / "pune_data.xlsx"
HP_SHEET = "HP"
RSU_MASTER_TEMPLATE = SCRIPT_DIR / "RSU Master.csv"

# Frappe template columns (exact match with ERPNext import template)
TEMPLATE_COLUMNS = [
    "ID",
    "RSU Code",
    "SDU Billing Rates",
    "Circle",
    "GST State",
    "ID (Rsu Items)",
    "Item (Rsu Items)",
    "Plan FAT (Rsu Items)",
    "Plan Fibre length (Rsu Items)",
    "Plan HP (Rsu Items)",
]

FIBRE_TYPES = ["6F", "12F", "24F", "48F"]
ITEMS = [f"SER-FSDU-CFT-{ft}" for ft in FIBRE_TYPES]

# Cluster ID -> RSU Code override (when Site name differs from cluster_id-0)
RSU_CODE_OVERRIDES = {"SHT": "IPS,1WB,SHT-0"}

LOG_FILE = SCRIPT_DIR / "plan_hp_fat_unmatched_log.txt"
# Fallback when Site API returns 417: load valid cluster IDs from this CSV
# Export Site (filters: custom_lob=FTTH SDU, custom_type=RSU Cluster) from ERPNext
SITE_EXPORT_CSV = SCRIPT_DIR / "Site_valid_clusters.csv"


def _load_valid_cluster_ids_from_csv() -> set:
    """Load valid cluster IDs from Site export CSV (when API returns 417)."""
    if not SITE_EXPORT_CSV.exists():
        raise FileNotFoundError(
            f"{SITE_EXPORT_CSV} not found. Export Site (custom_lob=FTTH SDU, custom_type=RSU Cluster) from ERPNext."
        )
    df = pd.read_csv(SITE_EXPORT_CSV, encoding="utf-8-sig")
    df.columns = df.columns.str.replace("\ufeff", "").str.strip()
    valid = set()
    cols_to_try = ("name", "Name", "cluster_id", "cluster id", "Cluster ID")
    used_col = None
    for col in cols_to_try:
        if col in df.columns:
            used_col = col
            break
    if used_col is None and len(df.columns) > 0:
        used_col = df.columns[0]
    if used_col is not None:
        for v in df[used_col].dropna().astype(str).str.strip():
            if v and v.lower() != "nan":
                norm = normalize_cluster_id(v)
                valid.add(norm.upper())
                valid.add(cluster_id_to_site_match(norm))
    if not valid:
        print("WARNING: Site_valid_clusters.csv is empty. Add Site names (or cluster_id) from ERPNext.")
    return valid


def normalize_cluster_id(s) -> str:
    """Remove all spaces from Cluster ID. E.g. 'nh0  - 1-0' -> 'nh0-1-0'"""
    return re.sub(r"\s+", "", str(s).strip()) if pd.notna(s) else ""


def cluster_id_to_site_match(s) -> str:
    """Normalize, append -0, uppercase for case-insensitive Site comparison. E.g. '0d3' -> '0D3-0'"""
    norm = normalize_cluster_id(s)
    if not norm:
        return ""
    base = f"{norm}-0" if not norm.endswith("-0") else norm
    return base.upper()


def fetch_valid_cluster_ids_from_site() -> set:
    """
    Fetch Site doctype with custom_lob='FTTH SDU' and custom_type='RSU Cluster'.
    Returns set of normalized Cluster IDs (name / cluster_id) that match.
    Tries API; on 417, falls back to Site_valid_clusters.csv.
    """
    base_url, _ = get_erp_session()
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    api_key = os.getenv("NEXT_PUBLIC_FRAPPE_API_KEY")
    api_secret = os.getenv("NEXT_PUBLIC_FRAPPE_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("ERPNext credentials missing in .env")

    valid = set()
    # Fetch Sites (no filters in URL - shorter URL may avoid 417), filter in Python
    limit = 200
    for start in range(0, 20000, limit):
        try:
            qs = urllib.parse.urlencode({
                "limit_page_length": limit,
                "limit_start": start,
                "fields": json.dumps(["name", "cluster_id", "custom_lob", "custom_type"], separators=(",", ":")),
            })
            url = f"{base_url}/api/resource/Site?{qs}"
            req = urllib.request.Request(url, headers={
                "Authorization": f"token {api_key}:{api_secret}",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if SITE_EXPORT_CSV.exists():
                print(f"Site API returned {e.code} - using {SITE_EXPORT_CSV}")
                return _load_valid_cluster_ids_from_csv()
            raise RuntimeError(
                f"Site API failed ({e.code}). Create {SITE_EXPORT_CSV}:\n"
                "  1. In ERPNext, open Site list\n"
                "  2. Filter: custom_lob=FTTH SDU, custom_type=RSU Cluster\n"
                "  3. Export as CSV, save as Site_valid_clusters.csv\n"
                "  4. CSV must have a 'name' or 'cluster_id' column"
            ) from e

        items = data.get("data") or data.get("results") or []
        if not items:
            break
        for item in items:
            # Filter: custom_lob=FTTH SDU, custom_type=RSU Cluster
            if (str(item.get("custom_lob") or "").strip() != "FTTH SDU" or
                str(item.get("custom_type") or "").strip() != "RSU Cluster"):
                continue
        val = item.get("cluster_id") or item.get("name") or ""
        if val:
            valid.add(normalize_cluster_id(val))
        if item.get("name"):
            valid.add(normalize_cluster_id(item["name"]))
    return valid


def get_erp_session():
    """Create a session for ERPNext API using credentials from .env."""
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    base_url = os.getenv("NEXT_PUBLIC_FRAPPE_URL", "").rstrip("/")
    api_key = os.getenv("NEXT_PUBLIC_FRAPPE_API_KEY")
    api_secret = os.getenv("NEXT_PUBLIC_FRAPPE_API_SECRET")

    if not base_url or not api_key or not api_secret:
        raise RuntimeError("ERPNext credentials missing in .env")

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"token {api_key}:{api_secret}",
            "Content-Type": "application/json",
            "Expect": "",  # Avoid 417 Expectation Failed
        }
    )
    # Prevent Expect: 100-continue (causes 417 with Frappe Cloud proxy)
    from urllib3.util.retry import Retry
    adapter = requests.adapters.HTTPAdapter(max_retries=Retry(0))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return base_url, session


def fetch_sdu_billing_by_circle(circles) -> dict:
    """
    Fetch SDU Billing Master name for each Circle via ERPNext API.
    Returns mapping {circle: sdu_billing_name}.
    """
    base_url, session = get_erp_session()
    sdu_map = {}

    for circle in sorted(set(str(c).strip() for c in circles if pd.notna(c) and str(c).strip())):
        if circle in sdu_map:
            continue
        try:
            url = f"{base_url}/api/resource/SDU Billing Master"
            params = {"filters": f'[["SDU Billing Master","circle","=","{circle}"]]'}
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data") or data.get("results") or []
            sdu_map[circle] = items[0].get("name", "") if items else ""
        except Exception:
            sdu_map[circle] = ""

    return sdu_map


def build_rsu_rows(source_df: pd.DataFrame, sdu_billing_map: dict, valid_cluster_ids: set) -> tuple:
    """
    Build RSU Master rows in Frappe template format.
    Only includes clusters that exist in Site. Unmatched clusters are excluded from output.
    Returns (rows_df, unmatched_cluster_ids).
    """
    cluster_groups = source_df.groupby("Cluster ID").first().reset_index()
    cluster_groups["Cluster ID"] = cluster_groups["Cluster ID"].astype(str).str.strip()
    cluster_groups["Cluster ID for Site Match"] = cluster_groups["Cluster ID"].apply(cluster_id_to_site_match)
    valid_upper = {str(x).upper() for x in valid_cluster_ids}

    unmatched = []
    rows = []

    for _, row in cluster_groups.iterrows():
        cluster_id = row["Cluster ID"]
        site_match = row["Cluster ID for Site Match"]
        if not site_match or site_match.upper() not in valid_upper:
            unmatched.append(cluster_id)
            continue

        circle = row["Circle"]
        circle_str = str(circle).strip() if pd.notna(circle) else ""
        sdu_billing = sdu_billing_map.get(circle_str, "")

        plan_fat = int(row["Planned FAT"]) if pd.notna(row["Planned FAT"]) else 0
        plan_hp = int(row["Planned HP"]) if pd.notna(row["Planned HP"]) else 0

        norm = normalize_cluster_id(cluster_id)
        rsu_code_base = f"{norm}-0" if not norm.endswith("-0") else norm
        rsu_code = RSU_CODE_OVERRIDES.get(cluster_id, rsu_code_base)
        for i, item in enumerate(ITEMS):
            r = {
                "ID": "" if i == 0 else "",
                "RSU Code": rsu_code if i == 0 else "",
                "SDU Billing Rates": sdu_billing if i == 0 else "",
                "Circle": circle if i == 0 else "",
                "GST State": "",
                "ID (Rsu Items)": "",
                "Item (Rsu Items)": item,
                "Plan FAT (Rsu Items)": plan_fat,
                "Plan Fibre length (Rsu Items)": 0,
                "Plan HP (Rsu Items)": plan_hp,
            }
            rows.append(r)

    return pd.DataFrame(rows, columns=TEMPLATE_COLUMNS) if rows else pd.DataFrame(columns=TEMPLATE_COLUMNS), unmatched


def main():
    if not SOURCE_XLSX.exists():
        print(f"Error: {SOURCE_XLSX} not found.")
        return

    # Read pune_data.xlsx sheet "HP" (row 0=section labels, row 1=header, row 2+=data)
    source_raw = pd.read_excel(SOURCE_XLSX, sheet_name=HP_SHEET, header=None)
    data = source_raw.iloc[2:]  # skip header rows
    source = pd.DataFrame({
        "Circle": data.iloc[:, 1],
        "Planned FAT": data.iloc[:, 6],
        "Planned HP": data.iloc[:, 9],
        "Cluster ID": data.iloc[:, 14],
    })

    source = source[source["Cluster ID"].astype(str).str.strip() != "Cluster ID"].dropna(subset=["Cluster ID"])
    source["Cluster ID"] = source["Cluster ID"].astype(str).str.strip()

    valid_cluster_ids = fetch_valid_cluster_ids_from_site()
    sdu_billing_map = fetch_sdu_billing_by_circle(source["Circle"].unique())
    new_rows, unmatched = build_rsu_rows(source, sdu_billing_map, valid_cluster_ids)

    # Log unmatched Cluster IDs
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"plan_hp_fat.py - Unmatched Cluster IDs (not found in Site)\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Filters: Site custom_lob='FTTH SDU', custom_type='RSU Cluster'\n")
        f.write("-" * 60 + "\n")
        if unmatched:
            for cid in sorted(set(unmatched)):
                f.write(f"  {cid}\n")
            f.write(f"\nTotal unmatched: {len(set(unmatched))}\n")
        else:
            f.write("  (none)\n")

    # Output only matched clusters
    output_path = SCRIPT_DIR / "RSU Master Import.csv"
    new_rows.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    matched = len(new_rows) // len(ITEMS) if len(ITEMS) else 0
    print(f"Saved to {output_path}")
    print(f"Matched clusters (added to CSV): {matched}")
    print(f"Unmatched clusters (see {LOG_FILE}): {len(set(unmatched))}")


if __name__ == "__main__":
    main()
