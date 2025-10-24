from __future__ import annotations
import pandas as pd
from pathlib import Path
from .config import Config
from .db import DB

def fetch_table():
    cfg = Config.from_env()
    db = DB(cfg)
    coll = db.db_mutual["daily_movement"]
    # Get date for last 9 days with data
    min_date = pd.Timestamp.now() - pd.Timedelta(days=9)
    # Get the latest 7 unique dates (from nested Date)
    docs = list(coll.find({"Date": {"$gte": min_date}}, {"_id": 0, "Scheme Name": 1, "Date": 1, "value": 1}).sort("Date", -1))
    if not docs:
        print("No data found.")
        return
    df = pd.DataFrame(docs)
    # Pivot: rows=Scheme Name, cols=Date, values=value
    table = df.pivot_table(index="Scheme Name", columns="Date", values="value", aggfunc="sum", fill_value=0)
    # Sort columns (dates) descending
    table = table.reindex(sorted(table.columns, reverse=True), axis=1)

    # if any value is zero, copy from the next (more recent) date
    for i in range(len(table.columns) - 2, -1, -1):
        col = table.columns[i]
        next_col = table.columns[i + 1]
        table[col] = table[col].where(table[col] != 0, table[next_col])

    # Keep a numeric copy for sorting and CSV export
    numeric = table.copy()
    # Ensure integer type for date columns
    if len(numeric.columns) > 0:
        numeric[numeric.columns] = numeric[numeric.columns].astype(int)

    # Compute change between latest two dates (numeric)
    date_cols = list(numeric.columns)
    if len(date_cols) >= 2:
        latest, prev = date_cols[0], date_cols[1]
        numeric["Change"] = numeric[latest].fillna(0).astype(int) - numeric[prev].fillna(0).astype(int)
    else:
        latest = None
        numeric["Change"] = 0

    # Move 'Change' to the first column (numeric)
    cols = ["Change"] + [c for c in numeric.columns if c != "Change"]
    numeric = numeric[cols]

    # Sort by latest date (descending) using numeric values
    if latest is not None:
        numeric = numeric.sort_values(by=latest, ascending=False)

    # Append TOTAL row at bottom
    total_row = numeric.drop(columns=["Change"]).sum(axis=0)
    if latest is not None:
        total_change = int(numeric[latest].sum() - numeric[prev].sum())
    else:
        total_change = int(0)
    total_series = pd.Series({"Change": total_change}, name="TOTAL")
    total_series = pd.concat([total_series, total_row])
    numeric_with_total = pd.concat([numeric, total_series.to_frame().T])

    # Save numeric CSV to data folder (overwrite)
    data_dir = (Path(__file__).resolve().parent.parent / "data")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_path = data_dir / "report_table.csv"
    numeric_with_total.to_csv(csv_path, index=True, index_label="Scheme Name")

    # Build display table from numeric_with_total with Indian number format (Lakhs, Crores)
    display = numeric_with_total.copy()
    
    # Format column headers to show only date (remove time)
    new_columns = []
    for col in display.columns:
        if col == "Change":
            new_columns.append(col)
        else:
            # Convert timestamp to date string
            if hasattr(col, 'strftime'):
                new_columns.append(col.strftime('%Y-%m-%d'))
            else:
                # Handle case where col might already be a string
                col_str = str(col)
                if ' ' in col_str:  # Remove time part if present
                    new_columns.append(col_str.split(' ')[0])
                else:
                    new_columns.append(col_str)
    display.columns = new_columns

    # Restrict index length to 40 chars
    display.index = display.index.map(lambda x: str(x) if len(str(x)) <= 40 else str(x)[:37] + "...")
    
    def format_indian_currency(value):
        """Format number in Indian currency format with proper comma placement"""
        if pd.isnull(value):
            return ""
        
        value = int(value)
        if value == 0:
            return "0"
        
        # Handle negative values
        is_negative = value < 0
        value = abs(value)
        
        # Convert to string for manipulation
        value_str = str(value)
        
        # Indian numbering system: x,xx,xx,xxx (group by 2 after first 3 digits from right)
        if len(value_str) <= 3:
            formatted = value_str
        else:
            # Split into groups: first group of 3, then groups of 2
            result = []
            remaining = value_str
            
            # Take last 3 digits
            if len(remaining) >= 3:
                result.append(remaining[-3:])
                remaining = remaining[:-3]
            else:
                result.append(remaining)
                remaining = ""
            
            # Take groups of 2 from right to left
            while remaining:
                if len(remaining) >= 2:
                    result.append(remaining[-2:])
                    remaining = remaining[:-2]
                else:
                    result.append(remaining)
                    remaining = ""
            
            # Reverse and join with commas
            result.reverse()
            formatted = ",".join(result)
        
        return f"-{formatted}" if is_negative else formatted
    
    for col in display.columns:
        display[col] = display[col].map(format_indian_currency)

    # Pretty print
    try:
        from tabulate import tabulate
        # Set column alignment - left for scheme names, right for all values
        colalign = ["left"] + ["right"] * len(display.columns)  # First column (index) left, rest right
        print(tabulate(display, headers="keys", tablefmt="fancy_grid", showindex=True, 
                      numalign="right", stralign="right", colalign=colalign))
    except ImportError:
        print(display.to_string())

if __name__ == "__main__":
    fetch_table()
