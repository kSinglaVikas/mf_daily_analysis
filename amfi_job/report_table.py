from __future__ import annotations
import pandas as pd
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
    table = df.pivot_table(index="category_name", columns="Date", values="value", aggfunc="sum", fill_value=0)
    # Sort columns (dates) descending
    table = table.reindex(sorted(table.columns, reverse=True), axis=1)
    # Compute change between latest two dates
    date_cols = list(table.columns)
    if len(date_cols) >= 2:
        latest, prev = date_cols[0], date_cols[1]
        # Compute numeric change (int, not formatted)
        table["Change"] = table[latest].fillna(0).astype(int) - table[prev].fillna(0).astype(int)
    else:
        table["Change"] = ""

    # Move 'Change' to the first column
    cols = ["Change"] + [c for c in table.columns if c != "Change"]
    table = table[cols]

    # Sort by latest date (descending)
    if len(date_cols) >= 1:
        table = table.sort_values(by=latest, ascending=False)

    # Format numbers with thousands separator (except for index)
    for col in table.columns:
        if col != "Change":
            table[col] = table[col].map(lambda x: f"{int(x):,}" if pd.notnull(x) and x != "" else "")
        elif col == "Change":
            table[col] = table[col].map(lambda x: f"{int(x):,}" if isinstance(x, (int, float)) and pd.notnull(x) else "")

    # Try to use tabulate for pretty print
    try:
        from tabulate import tabulate
        print(tabulate(table, headers="keys", tablefmt="fancy_grid", showindex=True, numalign="right", stralign="left"))
    except ImportError:
        print(table.to_string())

if __name__ == "__main__":
    fetch_table()
