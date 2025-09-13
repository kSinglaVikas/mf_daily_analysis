from __future__ import annotations
import io
import pandas as pd


def parse_nav_text(text: str) -> pd.DataFrame:

    # Accepts bytes (Excel file content)
    if isinstance(text, bytes):
        buf = io.BytesIO(text)
        df = pd.read_excel(buf, dtype=str)
    else:
        # fallback for text/csv
        buf = io.StringIO(text)
        df = pd.read_csv(buf, sep=None, engine="python", dtype=str, na_filter=False, comment="#")

    # Normalize column names to a canonical snake_case
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    # New Excel columns
    rename_map = {
        "MF_Id": "mf_id",
        "MF_Name": "mf_name",
        "SchemeType_id": "scheme_type_id",
        "SchemeType_Desc": "scheme_type_desc",
        "SchemeCat_Id": "scheme_cat_id",
        "SchemeCat_Desc": "scheme_cat_desc",
        "Scheme_ID": "scheme_id",
        "Scheme_Name": "scheme_name",
        "SD_Id": "scheme_code",
        "NAV_Name": "nav_name",
        "hNAV_Date": "nav_date",
        "hNAV_Amt": "nav_amt",
        "ISIN_RI": "isin_ri",
        "ISIN_PO": "isin_po",
    }

    for src, dst in rename_map.items():
        if src in df.columns:
            df.rename(columns={src: dst}, inplace=True)

    # Trim spaces
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()


    # Convert nav_amt to float where possible
    if "nav_amt" in df.columns:
        df["nav_amt"] = pd.to_numeric(df["nav_amt"].str.replace(",", "", regex=False), errors="coerce")

    # Convert scheme_code to int where possible
    if "scheme_code" in df.columns:
        df["scheme_code"] = pd.to_numeric(df["scheme_code"], errors="coerce").astype('Int64')

    # Parse nav_date to date format from yyyy-mm-dd
    if "nav_date" in df.columns:
        df["nav_date"] = pd.to_datetime(df["nav_date"], errors="coerce", format="%Y-%m-%d")

    # Drop rows without scheme_id or nav_amt
    if "scheme_id" in df.columns:
        df = df[df["scheme_id"].notna() & (df["scheme_id"].astype(str) != "")]

    return df


def minimal_nav(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in ["scheme_code", "scheme_name", "nav_amt", "nav_date"] if c in df.columns]
    return df[keep].copy()
