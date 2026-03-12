"""
Summarize PO length by OLT ID and Fiber Capacity from pune_data.xlsx (sheet "Route-Details").

Output columns:
- OLT ID
- Fiber Capacity
- PO length
"""

from pathlib import Path
import pandas as pd


SCRIPT_DIR = Path(__file__).parent
SOURCE_XLSX = SCRIPT_DIR / "pune_data.xlsx"
SHEET_NAME = "Route-Details"
OUTPUT_XLSX = SCRIPT_DIR / "OLT_PO_Length_Summary.xlsx"


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

    needed = ["OLT ID", "Fiber Capacity", "PO length"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in sheet '{SHEET_NAME}': {missing}")

    return df[needed].copy()


def main() -> None:
    df = load_route_details()

    # Clean values
    df["OLT ID"] = df["OLT ID"].astype(str).str.strip()
    df["Fiber Capacity"] = df["Fiber Capacity"].astype(str).str.strip()

    # PO length numeric (keep NaN so we can detect empty groups)
    df["PO length"] = pd.to_numeric(df["PO length"], errors="coerce")

    def _group_po_length(s: pd.Series):
        vals = s.dropna().unique()
        if len(vals) == 0:
            return ""
        if len(vals) == 1:
            return vals[0]
        # If multiple different PO lengths exist for same group, keep unique values
        # (not summed) so you can review.
        return "; ".join(str(v) for v in sorted(vals))

    summary = (
        df.groupby(["OLT ID", "Fiber Capacity"], dropna=False, sort=True)["PO length"]
        .apply(_group_po_length)
        .reset_index()
    )

    # Display "OLT ID" only once per group (like a merged-cell view)
    summary = summary.sort_values(["OLT ID", "Fiber Capacity"], kind="stable").reset_index(drop=True)
    summary.loc[summary["OLT ID"].duplicated(), "OLT ID"] = ""

    # Write Excel
    summary.to_excel(OUTPUT_XLSX, index=False)
    print(f"Saved: {OUTPUT_XLSX}")
    print(f"Rows: {len(summary)}")


if __name__ == "__main__":
    main()

