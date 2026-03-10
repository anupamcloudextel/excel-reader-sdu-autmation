"""
Create RSU Master documents (with RSU Item child rows) via Frappe API
from Agra.xlsx. Use this when the Data Import UI cannot match child table columns.
"""

import os
import requests
import pandas as pd
from pathlib import Path

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).parent
AGRA_XLSX = SCRIPT_DIR / "Agra.xlsx"

# Frappe connection (from .env or set here)
FRAPPE_URL = os.getenv("NEXT_PUBLIC_FRAPPE_URL", "").rstrip("/")
API_KEY = os.getenv("NEXT_PUBLIC_FRAPPE_API_KEY", "")
API_SECRET = os.getenv("NEXT_PUBLIC_FRAPPE_API_SECRET", "")

FIBRE_TYPES = ["6F", "12F", "24F", "48F"]
ITEMS = [f"SER-FSDU-CFT-{ft}" for ft in FIBRE_TYPES]


def get_agra_clusters():
    """Read Agra.xlsx and return list of cluster dicts: cluster_id, circle, plan_fat, plan_hp."""
    df = pd.read_excel(AGRA_XLSX, header=0)
    agra = pd.DataFrame({
        "Circle": df.iloc[:, 1],
        "Planned FAT": df.iloc[:, 6],
        "Planned HP": df.iloc[:, 9],
        "Cluster ID": df.iloc[:, 14],
    })
    agra = agra[agra["Cluster ID"].astype(str).str.strip() != "Cluster ID"].dropna(subset=["Cluster ID"])
    agra["Cluster ID"] = agra["Cluster ID"].astype(str).str.strip()
    clusters = agra.groupby("Cluster ID").first().reset_index()
    return [
        {
            "cluster_id": str(r["Cluster ID"]).strip(),
            "circle": r["Circle"],
            "plan_fat": int(r["Planned FAT"]) if pd.notna(r["Planned FAT"]) else 0,
            "plan_hp": int(r["Planned HP"]) if pd.notna(r["Planned HP"]) else 0,
        }
        for _, r in clusters.iterrows()
    ]


def build_rsu_master_payload(cluster):
    """Build one RSU Master doc payload for API."""
    rsu_code = f"{cluster['cluster_id']}-0"
    rsu_item = [
        {
            "item": item,
            "plan_fat": cluster["plan_fat"],
            "plan_hp": cluster["plan_hp"],
            "plan_fibre_length": 0,
        }
        for item in ITEMS
    ]
    return {
        "rsu_code": rsu_code,
        "sdu_billing": "",
        "circle": cluster["circle"],
        "gst_state": "",
        "rsu_item": rsu_item,
    }


def main():
    if not FRAPPE_URL or not API_KEY or not API_SECRET:
        print("Set NEXT_PUBLIC_FRAPPE_URL, NEXT_PUBLIC_FRAPPE_API_KEY, NEXT_PUBLIC_FRAPPE_API_SECRET in .env")
        return
    if not AGRA_XLSX.exists():
        print(f"Error: {AGRA_XLSX} not found.")
        return

    clusters = get_agra_clusters()
    url = f"{FRAPPE_URL}/api/resource/RSU Master"
    headers = {
        "Authorization": f"token {API_KEY}:{API_SECRET}",
        "Content-Type": "application/json",
    }

    created = 0
    for cluster in clusters:
        payload = build_rsu_master_payload(cluster)
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
            if r.status_code in (200, 201):
                created += 1
                print(f"Created: {payload['rsu_code']} ({cluster['circle']})")
            else:
                print(f"Failed {payload['rsu_code']}: {r.status_code} {r.text[:200]}")
        except Exception as e:
            print(f"Error {payload['rsu_code']}: {e}")

    print(f"Done. Created {created} of {len(clusters)} RSU Master documents.")


if __name__ == "__main__":
    main()
