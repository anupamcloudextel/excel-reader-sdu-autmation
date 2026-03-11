"""
Extract data from HP.xlsx and generate CSV in Frappe RSU Master import template format.
Output matches RSU Master.csv template for Data Import (ERPNext v15).
"""

import csv
import pandas as pd
from pathlib import Path

# File paths
SCRIPT_DIR = Path(__file__).parent
SOURCE_XLSX = SCRIPT_DIR / "HP.xlsx"
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


def build_rsu_rows(source_df: pd.DataFrame) -> pd.DataFrame:
    """Build RSU Master rows in Frappe template format."""
    cluster_groups = source_df.groupby("Cluster ID").first().reset_index()
    cluster_groups["Cluster ID"] = cluster_groups["Cluster ID"].astype(str).str.strip()

    rows = []
    for _, row in cluster_groups.iterrows():
        cluster_id = row["Cluster ID"]
        circle = row["Circle"]
        plan_fat = int(row["Planned FAT"]) if pd.notna(row["Planned FAT"]) else 0
        plan_hp = int(row["Planned HP"]) if pd.notna(row["Planned HP"]) else 0

        rsu_code = RSU_CODE_OVERRIDES.get(cluster_id, f"{cluster_id}-0")
        for i, item in enumerate(ITEMS):
            r = {
                "ID": "" if i == 0 else "",
                "RSU Code": rsu_code if i == 0 else "",
                "SDU Billing Rates": "",
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

    # Read HP.xlsx
    source_raw = pd.read_excel(SOURCE_XLSX, header=0)
    source = pd.DataFrame({
        "Circle": source_raw.iloc[:, 1],
        "Planned FAT": source_raw.iloc[:, 6],
        "Planned HP": source_raw.iloc[:, 9],
        "Cluster ID": source_raw.iloc[:, 14],
    })

    source = source[source["Cluster ID"].astype(str).str.strip() != "Cluster ID"].dropna(subset=["Cluster ID"])
    source["Cluster ID"] = source["Cluster ID"].astype(str).str.strip()

    new_rows = build_rsu_rows(source)

    # Output only new rows in template format (for Data Import - Insert New Records)
    output_path = SCRIPT_DIR / "RSU Master Import.csv"
    new_rows.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

    print(f"Saved to {output_path}")
    print(f"Added {len(new_rows)} rows from {len(source['Cluster ID'].unique())} clusters.")
    print(f"Cluster IDs: {', '.join(sorted(source['Cluster ID'].unique()))}")


if __name__ == "__main__":
    main()
