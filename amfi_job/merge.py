from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any


def merge_nav_with_active(nav_df: pd.DataFrame, active_schemes: List[Dict[str, Any]]) -> pd.DataFrame:
    act_df = pd.DataFrame(active_schemes)

    merged = pd.merge(
        nav_df,
        act_df,
        left_on="scheme_code",
        right_on="categoryCode",
        how="inner",
        suffixes=("", "_active"),
    )

    # keep cols scheme_code, scheme_name, nav_amt, nav_date, activeUnits
    keep_cols = ["scheme_code", "scheme_name", "nav_amt", "nav_date", "activeUnits"]
    merged = merged[keep_cols]

    # rename to 'Scheme Code' and 'Date'
    merged.rename(columns={
        "scheme_code": "Scheme Code",
        "scheme_name": "Scheme Name",
        "nav_amt": "nav",
        "nav_date": "Date",
        "activeUnits": "Active Units",
    }, inplace=True)

    # Compute value = active_units * nav, ensure float or None (never NaN)
    merged["value"] = None
    if "Active Units" in merged.columns and "nav" in merged.columns:
        def safe_float(val, row, col):
            try:
                return float(str(val).replace(",", "").strip())
            except Exception as e:
                print(f"[DEBUG] Could not convert {col}='{val}' for row {row.name}: {e}")
                return None
        def rounded_value(row):
            if pd.notnull(row["Active Units"]) and pd.notnull(row["nav"]):
                au = safe_float(row["Active Units"], row, "Active Units")
                nv = safe_float(row["nav"], row, "nav")
                if au is not None and nv is not None:
                    return int(round(au * nv))
            return None
        merged["value"] = merged.apply(rounded_value, axis=1)
        # Debug: print rows where value is still NaN or None
        nan_mask = merged["value"].isna()
        if nan_mask.any():
            print("[DEBUG] Rows with NaN/None value after conversion:")
            debug_df = merged.loc[nan_mask, [c for c in [ "Scheme Code", "Scheme Name", "Active Units", "nav"] if c in merged.columns]]
            print(debug_df.to_string(index=False))

    preferred_cols = [
        "Scheme Code",
        "Scheme Name",
        "nav",
        "Date",
        "Active Units",
        "value",
    ]
    preferred_cols = [c for c in preferred_cols if c in merged.columns]

    # Deduplicate by 'Scheme Code' and 'Date' taking last
    merged = merged[preferred_cols].copy()
    merged.drop_duplicates(subset=["Scheme Code", "Date"], keep="last", inplace=True)

    return merged


def to_daily_movement_docs(df: pd.DataFrame) -> List[Dict[str, Any]]:

    records = df.to_dict(orient="records")
    # Ensure types and field names for MongoDB
    import pandas as pd
    import datetime
    for r in records:
        if isinstance(r.get("nav"), str):
            try:
                r["nav"] = float(r["nav"].replace(",", ""))
            except Exception:
                r["nav"] = None
        # Ensure 'Date' is a valid datetime
        d = r.get("Date")
        if pd.isna(d):
            r["Date"] = None
        elif not isinstance(d, datetime.datetime) and hasattr(d, 'to_pydatetime'):
            try:
                r["Date"] = d.to_pydatetime()
            except Exception:
                r["Date"] = None
        elif not isinstance(d, datetime.datetime):
            r["Date"] = None
        # Remove lowercase 'scheme_code' and 'date' to avoid confusion
        if "scheme_code" in r:
            del r["scheme_code"]
        if "date" in r:
            del r["date"]
    return records
