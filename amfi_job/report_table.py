from __future__ import annotations
import pandas as pd
from pathlib import Path
from .config import Config
from .db import DB

def fetch_table():
    cfg = Config.from_env()
    db = DB(cfg)
    coll = db.db_mutual["daily_movement"]
    # Get the latest 7 unique dates
    dates = list(coll.distinct("Date"))
    dates = sorted([d for d in dates if d is not None], reverse=True)[:7]
    # Query all docs for these dates
    docs = list(coll.find({"Date": {"$in": dates}}, {"_id": 0, "category_name": 1, "Date": 1, "value": 1}))
    if not docs:
        print("No data found.")
        return
    df = pd.DataFrame(docs)
    # Pivot: rows=category_name, cols=Date, values=value
    # Build numeric pivot table
    table = df.pivot_table(index="category_name", columns="Date", values="value", aggfunc="sum", fill_value=0)
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
    numeric_with_total.to_csv(csv_path, index=True, index_label="category_name")

    # Build display table from numeric_with_total with thousands separators
    display = numeric_with_total.copy()
    for col in display.columns:
        display[col] = display[col].map(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

    # Pretty print
    try:
        from tabulate import tabulate
        print(tabulate(display, headers="keys", tablefmt="fancy_grid", showindex=True, numalign="right", stralign="left"))
    except ImportError:
        print(display.to_string())

if __name__ == "__main__":
    fetch_table()
