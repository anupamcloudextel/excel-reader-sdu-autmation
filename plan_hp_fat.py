"""
Extract data from pune_data.xlsx sheet "HP" and generate CSV in Frappe RSU Master import template format.
Output matches RSU Master.csv template for Data Import (ERPNext v15).
"""

import csv
import os
import pandas as pd
import requests
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
        }
    )
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


def build_rsu_rows(source_df: pd.DataFrame, sdu_billing_map: dict) -> pd.DataFrame:
    """Build RSU Master rows in Frappe template format."""
    cluster_groups = source_df.groupby("Cluster ID").first().reset_index()
    cluster_groups["Cluster ID"] = cluster_groups["Cluster ID"].astype(str).str.strip()

    rows = []
    for _, row in cluster_groups.iterrows():
        cluster_id = row["Cluster ID"]
        circle = row["Circle"]
        circle_str = str(circle).strip() if pd.notna(circle) else ""
        sdu_billing = sdu_billing_map.get(circle_str, "")

        plan_fat = int(row["Planned FAT"]) if pd.notna(row["Planned FAT"]) else 0
        plan_hp = int(row["Planned HP"]) if pd.notna(row["Planned HP"]) else 0

        rsu_code = RSU_CODE_OVERRIDES.get(cluster_id, f"{cluster_id}-0")
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

    return pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)


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

    sdu_billing_map = fetch_sdu_billing_by_circle(source["Circle"].unique())
    new_rows = build_rsu_rows(source, sdu_billing_map)

    # Output only new rows in template format (for Data Import - Insert New Records)
    output_path = SCRIPT_DIR / "RSU Master Import.csv"
    new_rows.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    print(f"Saved to {output_path}")
    print(f"Added {len(new_rows)} rows from {len(source['Cluster ID'].unique())} clusters.")
    print(f"Cluster IDs: {', '.join(sorted(source['Cluster ID'].unique()))}")


if __name__ == "__main__":
    main()
