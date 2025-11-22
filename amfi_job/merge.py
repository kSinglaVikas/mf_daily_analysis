from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any


def _prepare_dataframes(nav_df: pd.DataFrame, active_schemes: List[Dict[str, Any]]) -> tuple:
    """Prepare and normalize dataframes for merging"""
    act_df = pd.DataFrame(active_schemes)
    nav_df_copy = nav_df.copy()
    nav_df_copy["scheme_code"] = pd.to_numeric(nav_df_copy["scheme_code"], errors="coerce").astype('Int64')
    act_df["categoryCode"] = pd.to_numeric(act_df["categoryCode"], errors="coerce").astype('Int64')
    return nav_df_copy, act_df


def _safe_float_conversion(val, row, col):
    """Safely convert a value to float"""
    try:
        return float(str(val).replace(",", "").strip())
    except Exception as e:
        print(f"[DEBUG] Could not convert {col}='{val}' for row {row.name}: {e}")
        return None


def _calculate_value(row):
    """Calculate value from Active Units and nav"""
    if pd.notnull(row["Active Units"]) and pd.notnull(row["nav"]):
        au = _safe_float_conversion(row["Active Units"], row, "Active Units")
        nv = _safe_float_conversion(row["nav"], row, "nav")
        if au is not None and nv is not None:
            return int(round(au * nv))
    return None


def _add_value_column(merged: pd.DataFrame) -> pd.DataFrame:
    """Add value column and handle NaN values"""
    merged["value"] = None
    if "Active Units" in merged.columns and "nav" in merged.columns:
        merged["value"] = merged.apply(_calculate_value, axis=1)
        nan_mask = merged["value"].isna()
        if nan_mask.any():
            print("[DEBUG] Rows with NaN/None value after conversion:")
            debug_df = merged.loc[nan_mask, [c for c in ["Scheme Code", "Scheme Name", "Active Units", "nav"] if c in merged.columns]]
            print(debug_df.to_string(index=False))
    return merged


def merge_nav_with_active(nav_df: pd.DataFrame, active_schemes: List[Dict[str, Any]]) -> pd.DataFrame:
    """Merge NAV data with active schemes"""
    nav_df_copy, act_df = _prepare_dataframes(nav_df, active_schemes)

    merged = pd.merge(
        nav_df_copy,
        act_df,  
        left_on="scheme_code",
        right_on="categoryCode",
        how="inner",
        suffixes=("", "_active"),
    )

    keep_cols = ["scheme_code", "scheme_name", "nav_amt", "nav_date", "activeUnits"]
    merged = merged[keep_cols]

    merged.rename(columns={
        "scheme_code": "Scheme Code",
        "scheme_name": "Scheme Name",
        "nav_amt": "nav",
        "nav_date": "Date",
        "activeUnits": "Active Units",
    }, inplace=True)

    merged = _add_value_column(merged)

    preferred_cols = [
        "Scheme Code",
        "Scheme Name",
        "nav",
        "Date",
        "Active Units",
        "value",
    ]
    preferred_cols = [c for c in preferred_cols if c in merged.columns]

    merged = merged[preferred_cols].copy()
    merged.drop_duplicates(subset=["Scheme Code", "Date"], keep="last", inplace=True)

    merged["Week of Year"] = merged["Date"].dt.isocalendar().week
    merged["Year"] = merged["Date"].dt.year

    return merged


def _convert_nav_to_float(r):
    """Convert nav field to float if it's a string"""
    if isinstance(r.get("nav"), str):
        try:
            r["nav"] = float(r["nav"].replace(",", ""))
        except Exception:
            r["nav"] = None


def _convert_date_field(r):
    """Ensure Date field is a valid datetime"""
    import datetime
    d = r.get("Date")
    if pd.isna(d):
        r["Date"] = None
        print(f"[DEBUG] 'Date' is NaN, setting to None for record: {r}")
    elif not isinstance(d, datetime.datetime) and hasattr(d, 'to_pydatetime'):
        try:
            r["Date"] = d.to_pydatetime()
        except Exception:
            r["Date"] = None
            print(f"[DEBUG] Could not convert 'Date'='{d}' for record: {r}")
    elif not isinstance(d, datetime.datetime):
        r["Date"] = None
        print(f"[DEBUG] 'Date' is not datetime, setting to None for record: {r}")


def _clean_record_fields(r):
    """Remove lowercase duplicate fields"""
    if "scheme_code" in r:
        del r["scheme_code"]
    if "date" in r:
        del r["date"]


def to_daily_movement_docs(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame to MongoDB documents"""
    print("[DEBUG] Converting DataFrame to daily movement documents")
    print(df.head(1).to_string(index=False))
    
    records = df.to_dict(orient="records")
    
    for r in records:
        _convert_nav_to_float(r)
        _convert_date_field(r)
        _clean_record_fields(r)
    
    return records
