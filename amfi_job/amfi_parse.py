from __future__ import annotations
import io
import pandas as pd


def parse_nav_text(text: str) -> pd.DataFrame:
    """Parse NAV text file from AMFI portal
    
    Args:
        text: Text content from AMFI portal (semicolon-separated values)
        
    Returns:
        DataFrame with normalized column names
    """
    
    # Handle text content with semicolon separator
    if isinstance(text, str):
        buf = io.StringIO(text)
        df = pd.read_csv(buf, sep=';', dtype=str, na_filter=False)
    else:
        # Fallback for bytes (shouldn't happen with new format)
        buf = io.BytesIO(text)
        df = pd.read_excel(buf, dtype=str)

    # Normalize column names to a canonical snake_case
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    # New text file columns mapping
    rename_map = {
        "Scheme Code": "scheme_code",
        "Scheme Name": "scheme_name",
        "ISIN Div Payout/ISIN Growth": "isin_po",
        "ISIN Div Reinvestment": "isin_ri", 
        "Net Asset Value": "nav_amt",
        "Repurchase Price": "repurchase_price",
        "Sale Price": "sale_price",
        "Date": "nav_date",
        # Keep legacy mappings for backward compatibility
        "MF_Id": "mf_id",
        "MF_Name": "mf_name",
        "SchemeType_id": "scheme_type_id",
        "SchemeType_Desc": "scheme_type_desc",
        "SchemeCat_Id": "scheme_cat_id",
        "SchemeCat_Desc": "scheme_cat_desc",
        "Scheme_ID": "scheme_id",
        "SD_Id": "scheme_code",
        "NAV_Name": "nav_name",
        "NAV_Date": "nav_date",
        "NAV_Amt": "nav_amt",
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

    # Convert scheme_code to int where possible, but keep as string if conversion fails
    if "scheme_code" in df.columns:
        # Try to convert to numeric, but keep original if it fails
        numeric_codes = pd.to_numeric(df["scheme_code"], errors="coerce")
        # If conversion is successful for all values, use Int64, otherwise keep as string
        if not numeric_codes.isna().any():
            df["scheme_code"] = numeric_codes.astype('Int64')
        else:
            # Keep as string but strip whitespace
            df["scheme_code"] = df["scheme_code"].astype(str).str.strip()

    # Parse nav_date to date format, supporting multiple formats
    if "nav_date" in df.columns:
        def parse_date(val):
            import pandas as pd
            try:
                # Try DD-MMM-YYYY (new format from portal)
                return pd.to_datetime(val, format="%d-%b-%Y", errors="raise")
            except Exception:
                try:
                    # Try DD-MM-YYYY
                    return pd.to_datetime(val, format="%d-%m-%Y", errors="raise")
                except Exception:
                    try:
                        # Try DD/MM/YYYY
                        return pd.to_datetime(val, format="%d/%m/%Y", errors="raise")
                    except Exception:
                        try:
                            # Try m/d/yyyy (legacy)
                            return pd.to_datetime(val, format="%m/%d/%Y", errors="raise")
                        except Exception:
                            try:
                                # Try yyyy-mm-dd (legacy)
                                return pd.to_datetime(val, format="%Y-%m-%d", errors="raise")
                            except Exception:
                                # Fallback to pandas default parser
                                return pd.to_datetime(val, errors="coerce")
        df["nav_date"] = df["nav_date"].apply(parse_date)

    # Drop rows without scheme_code or nav_amt
    if "scheme_code" in df.columns:
        df = df[df["scheme_code"].notna() & (df["scheme_code"].astype(str) != "")]
    elif "scheme_id" in df.columns:
        # Fallback for legacy format
        df = df[df["scheme_id"].notna() & (df["scheme_id"].astype(str) != "")]

    return df


def minimal_nav(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in ["scheme_code", "scheme_name", "nav_amt", "nav_date"] if c in df.columns]
    return df[keep].copy()
