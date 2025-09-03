from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any


def merge_nav_with_active(nav_df: pd.DataFrame, active_schemes: List[Dict[str, Any]]) -> pd.DataFrame:
    act_df = pd.DataFrame(active_schemes)
    # If scheme_code missing in active, try categoryCode as fallback
    # Always create 'Scheme Code' and 'Date' columns for MongoDB compatibility
    if "scheme_code" not in act_df.columns and "categoryCode" in act_df.columns:
        act_df["scheme_code"] = act_df["categoryCode"].astype(str)
    if "scheme_code" in nav_df.columns:
        nav_df["scheme_code"] = nav_df["scheme_code"].astype(str).str.strip()
    if "scheme_code" in act_df.columns:
        act_df["scheme_code"] = act_df["scheme_code"].astype(str).str.strip()

    merged = pd.merge(
        nav_df,
        act_df,
        on="scheme_code",
        how="inner",
        suffixes=("", "_active"),
    )

    # Map categoryCode/categoryName to output
    if "categoryCode" in merged.columns:
        merged["category_code"] = merged["categoryCode"]
    if "categoryName" in merged.columns:
        merged["category_name"] = merged["categoryName"]
    if "activeUnits" in merged.columns:
        merged["active_units"] = merged["activeUnits"]

    # Always create 'Scheme Code' and 'Date' for MongoDB index
    merged["Scheme Code"] = merged["scheme_code"]
    # Convert date string to datetime.datetime for 'Date' field (MongoDB compatible)
    import datetime
    if "date" in merged.columns:
        merged["Date"] = pd.to_datetime(merged["date"], errors="coerce")
        # Ensure all are datetime.datetime (not date)
        merged["Date"] = merged["Date"].apply(lambda d: datetime.datetime.combine(d.date(), datetime.time()) if pd.notnull(d) else None)
    else:
        merged["Date"] = pd.NaT

    # Compute value = active_units * nav, ensure float or None (never NaN)
    import numpy as np
    merged["value"] = None
    if "active_units" in merged.columns and "nav" in merged.columns:
        def safe_float(val, row, col):
            try:
                return float(str(val).replace(",", "").strip())
            except Exception as e:
                print(f"[DEBUG] Could not convert {col}='{val}' for row {row.name}: {e}")
                return None
        def rounded_value(row):
            if pd.notnull(row["active_units"]) and pd.notnull(row["nav"]):
                au = safe_float(row["active_units"], row, "active_units")
                nv = safe_float(row["nav"], row, "nav")
                if au is not None and nv is not None:
                    return int(round(au * nv))
            return None
        merged["value"] = merged.apply(rounded_value, axis=1)
        # Debug: print rows where value is still NaN or None
        nan_mask = merged["value"].isna()
        if nan_mask.any():
            print("[DEBUG] Rows with NaN/None value after conversion:")
            debug_df = merged.loc[nan_mask, [c for c in ["Scheme Code", "scheme_code", "scheme_name", "active_units", "nav"] if c in merged.columns]]
            print(debug_df.to_string(index=False))

    preferred_cols = [
        "Scheme Code",
        "scheme_code",
        "scheme_name",
        "nav",
        "Date",
        "date",
        "category_code",
        "category_name",
        "active_units",
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
    for r in records:
        if isinstance(r.get("nav"), str):
            try:
                r["nav"] = float(r["nav"].replace(",", ""))
            except Exception:
                r["nav"] = None
        # Remove lowercase 'scheme_code' and 'date' to avoid confusion
        if "scheme_code" in r:
            del r["scheme_code"]
        if "date" in r:
            del r["date"]
    return records
