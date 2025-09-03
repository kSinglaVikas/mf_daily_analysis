from __future__ import annotations
import io
import pandas as pd
from typing import Tuple

# AMFI NAVAll.txt is pipe-delimited with headers similar to:
# Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
# Real file uses ';' or ',' historically; latest uses ';' or '|' depending on source mirror.
# We'll detect delimiter automatically using pandas.read_csv with python engine and sep=None.

EXPECTED_COLUMNS = [
    "Scheme Code",
    "ISIN Div Payout/ ISIN Growth",
    "ISIN Div Reinvestment",
    "Scheme Name",
    "Net Asset Value",
    "Date",
]


def parse_nav_text(text: str) -> pd.DataFrame:
    buf = io.StringIO(text)
    df = pd.read_csv(
        buf,
        sep=None,  # auto-detect
        engine="python",
        dtype=str,
        na_filter=False,
        comment="#",
    )
    # Normalize column names to a canonical snake_case
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    rename_map = {
        "Scheme Code": "scheme_code",
        "ISIN Div Payout/ ISIN Growth": "isin_payout_or_growth",
        "ISIN Div Reinvestment": "isin_reinvestment",
        "Scheme Name": "scheme_name",
        "Net Asset Value": "nav",
        "Date": "date",
    }

    # Be tolerant to variants like 'Net Asset Value (Rs.)', 'NAV', etc.
    for k in list(rename_map.keys()):
        if k not in df.columns:
            # Try alternative headers
            alternatives = {
                "Net Asset Value": ["Net Asset Value (Rs.)", "NAV", "Net Asset Value (Rs)"],
                "Scheme Code": ["SchemeCode", "Code"],
                "Scheme Name": ["SchemeName", "Name"],
                "Date": ["NAV Date", "navDate", "NAVDate"],
            }.get(k, [])
            for alt in alternatives:
                if alt in df.columns:
                    df.rename(columns={alt: k}, inplace=True)
                    break

    # Apply final canonical names where available
    for src, dst in rename_map.items():
        if src in df.columns:
            df.rename(columns={src: dst}, inplace=True)

    # Trim spaces
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()

    # Convert nav to float where possible
    if "nav" in df.columns:
        df["nav"] = pd.to_numeric(df["nav"].str.replace(",", "", regex=False), errors="coerce")

    # Parse date to yyyy-mm-dd string
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")

    # Drop rows without scheme_code or nav
    if "scheme_code" in df.columns:
        df = df[df["scheme_code"].notna() & (df["scheme_code"].astype(str) != "")]

    return df


def minimal_nav(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in ["scheme_code", "scheme_name", "nav", "date"] if c in df.columns]
    return df[keep].copy()
