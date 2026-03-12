"""
Take a copy of RSU Master update.csv and update only Plan Fibre length (Rsu Items)
from pune_data.xlsx sheet "Route-Details". All other columns remain from the CSV.
A backup copy is saved as RSU Master update_backup.csv before processing.
"""

from pathlib import Path
import shutil

import pandas as pd
import re


SCRIPT_DIR = Path(__file__).parent
SOURCE_XLSX = SCRIPT_DIR / "pune_data.xlsx"
SHEET_NAME = "Route-Details"
RSU_MASTER_UPDATE_CSV = SCRIPT_DIR / "RSU Master update.csv"
RSU_MASTER_BACKUP_CSV = SCRIPT_DIR / "RSU Master update_backup.csv"
OUTPUT_XLSX = SCRIPT_DIR / "RSU Master update (PO updated).xlsx"
OUTPUT_CSV = SCRIPT_DIR / "RSU Master update (PO updated).csv"


def _normalize_no_spaces(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip())


def olt_to_rsu_code(cluster_id: str) -> str:
    """Map Cluster ID -> RSU Code by appending -0 (after removing spaces)."""
    base = _normalize_no_spaces(cluster_id)
    if not base or base.lower() == "nan":
        return ""
    return f"{base}-0" if not base.endswith("-0") else base


def fibre_capacity_to_item(cap: str) -> str:
    """Map 6F -> SER-FSDU-CFT-6F. If already prefixed, keep as-is."""
    c = _normalize_no_spaces(cap)
    if not c or c.lower() == "nan":
        return ""
    if c.startswith("SER-FSDU-CFT-"):
        return c
    return f"SER-FSDU-CFT-{c}"


def load_route_details() -> pd.DataFrame:
    if not SOURCE_XLSX.exists():
        raise FileNotFoundError(f"{SOURCE_XLSX} not found.")

    raw = pd.read_excel(SOURCE_XLSX, sheet_name=SHEET_NAME, header=None)

    # Row 0 is blank/section artifacts, row 1 is the actual header row.
    header = raw.iloc[1].astype(str).str.strip()
    df = raw.iloc[2:].copy()
    df.columns = header

    # Drop duplicate columns (can happen with merged headers)
    df = df.loc[:, ~df.columns.duplicated()]

    # Use Cluster ID instead of OLT ID as the grouping key
    needed = ["Cluster ID", "Fiber Capacity", "PO length"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in sheet '{SHEET_NAME}': {missing}")

    return df[needed].copy()


def build_po_lookup(route_df: pd.DataFrame) -> dict[tuple[str, str], float | str]:
    """
    Build lookup: (RSU Code, Item) -> PO length value.
    If multiple unique values exist, returns a ';'-joined string.
    """
    df = route_df.copy()
    # Derive RSU Code from Cluster ID (normalized + '-0')
    df["RSU Code"] = df["Cluster ID"].astype(str).map(olt_to_rsu_code)
    df["Item (Rsu Items)"] = df["Fiber Capacity"].astype(str).map(fibre_capacity_to_item)
    df["PO length"] = pd.to_numeric(df["PO length"], errors="coerce")

    def _group_po_length(s: pd.Series):
        vals = s.dropna().unique()
        if len(vals) == 0:
            return ""
        if len(vals) == 1:
            # store as number (int when possible)
            v = vals[0]
            try:
                iv = int(v)
                if float(iv) == float(v):
                    return iv
            except Exception:
                pass
            return float(v)
        return "; ".join(str(v) for v in sorted(vals))

    grouped = (
        df.groupby(["RSU Code", "Item (Rsu Items)"], dropna=False, sort=True)["PO length"]
        .apply(_group_po_length)
        .reset_index()
    )
    lookup: dict[tuple[str, str], float | str] = {}
    for _, r in grouped.iterrows():
        lookup[(str(r["RSU Code"]), str(r["Item (Rsu Items)"]))] = r["PO length"]
    return lookup


def load_rsu_master_update() -> pd.DataFrame:
    if not RSU_MASTER_UPDATE_CSV.exists():
        raise FileNotFoundError(f"{RSU_MASTER_UPDATE_CSV} not found.")
    return pd.read_csv(RSU_MASTER_UPDATE_CSV)


def update_plan_fibre_length(rsu_df: pd.DataFrame, po_lookup: dict) -> pd.DataFrame:
    """
    Update only 'Plan Fibre length (Rsu Items)' in RSU Master update rows.
    Handles blank RSU Code rows by carrying forward previous RSU Code.
    """
    df = rsu_df.copy()
    current_rsu = ""
    updated = []
    for _, row in df.iterrows():
        rsu_code = str(row.get("RSU Code", "")).strip()
        if rsu_code and rsu_code.lower() != "nan":
            current_rsu = rsu_code
        item = str(row.get("Item (Rsu Items)", "")).strip()
        key = (current_rsu, item)
        if current_rsu and item and key in po_lookup:
            row["Plan Fibre length (Rsu Items)"] = po_lookup[key]
        updated.append(row)
    return pd.DataFrame(updated, columns=df.columns)


def main() -> None:
    route_df = load_route_details()
    po_lookup = build_po_lookup(route_df)

    # Take a copy of RSU Master update.csv as backup before updating
    if RSU_MASTER_UPDATE_CSV.exists():
        shutil.copy2(RSU_MASTER_UPDATE_CSV, RSU_MASTER_BACKUP_CSV)
        print(f"Backup saved: {RSU_MASTER_BACKUP_CSV}")

    rsu_df = load_rsu_master_update()
    updated = update_plan_fibre_length(rsu_df, po_lookup)

    # Write outputs (same columns as RSU Master update.csv)
    updated.to_excel(OUTPUT_XLSX, index=False)
    updated.to_csv(OUTPUT_CSV, index=False)

    print(f"Saved: {OUTPUT_XLSX}")
    print(f"Saved: {OUTPUT_CSV}")
    print(f"Rows: {len(updated)}")


if __name__ == "__main__":
    main()

