"""
Build RSU Hoto CSV from HP.xlsx.

Reads HP.xlsx (cluster HP/FAT/HOTO data), reshapes it, and outputs rows
in the same column structure as RSU Hoto.csv, suitable for ERPNext import.
"""

from pathlib import Path
import pandas as pd
import os
import requests
from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).parent
HP_XLSX = SCRIPT_DIR / "HP.xlsx"

# Output file: NEW file, do not touch existing RSU Hoto.csv
OUTPUT_CSV = SCRIPT_DIR / "RSU Hoto Import from HP.csv"

# RSU Hoto template columns (exact match, including spaces and casing)
HOTO_COLUMNS = [
    "ID",
    "RSU ID",
    "RSU Code",
    "Hoto Date ",
    "ID (Executed)",
    "Executed FAT (Executed)",
    "Executed HP (Executed)",
    "Hoto Item (Executed)",
    "Incremental Hoto(fibre length) (Executed)",
]


def load_hp() -> pd.DataFrame:
    """Load HP.xlsx and return a cleaned DataFrame with proper column names."""
    if not HP_XLSX.exists():
        raise FileNotFoundError(f"{HP_XLSX} not found.")

    raw = pd.read_excel(HP_XLSX, header=0)

    # First row contains the actual header names; remaining rows are data
    header_row = raw.iloc[0]
    hp = raw.iloc[1:].copy()
    hp.columns = header_row

    # Normalise column names we'll use
    hp = hp.rename(
        columns={
            "Cluster ID": "Cluster ID",
            "Fiber Type": "Fibre Type",
            "HOTO Date": "HOTO Date",
            "Fiber Length as per HOTO": "Fibre Length as per HOTO",
            "Actual Home Pass": "Actual Home Pass",
            "Actual Deployed FAT": "Actual Deployed FAT",
        }
    )

    # Keep only rows that have a Cluster ID (drop any footer/blank lines)
    hp = hp.dropna(subset=["Cluster ID"])

    # Ensure consistent types
    hp["Cluster ID"] = hp["Cluster ID"].astype(str).str.strip()
    hp["Fibre Type"] = hp["Fibre Type"].astype(str).str.strip()

    return hp


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


def fetch_rsu_ids(rsu_codes) -> dict:
    """
    Fetch RSU Master name for each RSU Code via ERPNext API.
    Returns mapping {rsu_code: rsu_name}.
    """
    base_url, session = get_erp_session()
    rsu_map: dict[str, str] = {}

    for code in sorted(set(rsu_codes)):
        if pd.isna(code):
            continue
        rsu_code = str(code).strip()
        if not rsu_code or rsu_code in rsu_map:
            continue

        try:
            # Assuming Doctype name is "RSU Master" and field `rsu_code`
            url = f"{base_url}/api/resource/RSU Master"
            params = {"filters": f'[["RSU Master","rsu_code","=","{rsu_code}"]]'}
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            # Standard Frappe list structure: data -> list of docs with "name"
            items = data.get("data") or data.get("results") or []
            if items:
                rsu_map[rsu_code] = items[0].get("name", "")
            else:
                rsu_map[rsu_code] = ""
        except Exception:
            # Fail soft: leave RSU ID empty for this code
            rsu_map[rsu_code] = ""

    return rsu_map


def build_hoto_rows(hp: pd.DataFrame) -> pd.DataFrame:
    """
    Map HP.xlsx data into RSU Hoto import format.

    Rules:
    - ID, RSU ID, ID (Executed) left blank
    - RSU Code = Cluster ID
    - Executed HP (Executed) = Actual Home Pass
    - Executed FAT (Executed) = Actual Deployed FAT
    - Hoto Date  = HOTO Date
    - Hoto Item (Executed) = SER-FSDU-CFT-<Fibre Type>
    - Incremental Hoto(fibre length) (Executed) = Fibre Length as per HOTO
    """
    # RSU Code is derived from Cluster ID (same format as existing data: e.g. "K38-0")
    # From your RSU Master.csv sample the pattern is "<ClusterID>-0"
    hp = hp.copy()
    hp["RSU Code"] = hp["Cluster ID"].astype(str).str.strip() + "-0"

    # Pre-fetch RSU IDs from ERPNext based on RSU Code
    rsu_id_map = fetch_rsu_ids(hp["RSU Code"].unique())

    rows = []

    # Group so that RSU Code and Hoto Date appear only on first row per (ClusterID, HOTO Date)
    grouped = hp.groupby(["Cluster ID", "HOTO Date"], dropna=False, sort=False)

    for (_, hoto_date), group in grouped:
        # Use same RSU Code for the whole group
        cluster_id = group["Cluster ID"].iloc[0]
        rsu_code = group["RSU Code"].iloc[0]
        rsu_id = rsu_id_map.get(rsu_code, "")

        hoto_date_out = pd.to_datetime(hoto_date).date() if not pd.isna(hoto_date) else ""

        for idx, row in group.reset_index(drop=True).iterrows():
            fibre_type = row["Fibre Type"]
            fibre_length = row["Fibre Length as per HOTO"]
            actual_hp = row["Actual Home Pass"]
            actual_fat = row["Actual Deployed FAT"]

            hoto_item = f"SER-FSDU-CFT-{fibre_type}"

            # Only first row of the group gets ID / RSU ID / RSU Code / Hoto Date
            if idx == 0:
                id_val = f"{cluster_id}-{hoto_date_out}" if hoto_date_out else ""
                rsu_id_val = rsu_id
                rsu_code_val = rsu_code
                hoto_date_val = hoto_date_out
            else:
                id_val = ""
                rsu_id_val = ""
                rsu_code_val = ""
                hoto_date_val = ""

            r = {
                "ID": id_val,
                "RSU ID": rsu_id_val,
                "RSU Code": rsu_code_val,
                "Hoto Date ": hoto_date_val,
                "ID (Executed)": "",
                "Executed FAT (Executed)": int(actual_fat) if pd.notna(actual_fat) else 0,
                "Executed HP (Executed)": int(actual_hp) if pd.notna(actual_hp) else 0,
                "Hoto Item (Executed)": hoto_item,
                "Incremental Hoto(fibre length) (Executed)": int(fibre_length) if pd.notna(fibre_length) else 0,
            }
            rows.append(r)

    return pd.DataFrame(rows, columns=HOTO_COLUMNS)


def main() -> None:
    hp = load_hp()
    hoto_rows = build_hoto_rows(hp)

    hoto_rows.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved HOTO import file to: {OUTPUT_CSV}")
    print(f"Total rows: {len(hoto_rows)}")
    print(f"Clusters: {', '.join(sorted(hp['Cluster ID'].unique()))}")


if __name__ == "__main__":
    main()

