from __future__ import annotations
import sys
from typing import Optional

from .config import Config
from .amfi_fetch import fetch_nav_text
from .amfi_parse import parse_nav_text, minimal_nav
from .db import DB
from .merge import merge_nav_with_active, to_daily_movement_docs


def run_once(verbose: bool = True) -> Optional[dict]:
    cfg = Config.from_env()

    if verbose:
        print("Fetching AMFI NAV file...")
    text = fetch_nav_text(cfg)

    if verbose:
        print("Parsing NAV file...")
    nav_df = parse_nav_text(text)
    nav_df = minimal_nav(nav_df)

    if verbose:
        print("Loading active schemes from MongoDB...")
    db = DB(cfg)
    active = db.get_active_schemes()

    if verbose:
        print(f"Merging {len(nav_df)} nav rows with {len(active)} active schemes...")
    merged_df = merge_nav_with_active(nav_df, active)

    docs = to_daily_movement_docs(merged_df)

    if verbose:
        print(f"Upserting {len(docs)} documents into mutualFunds.daily_movement...")
    result = db.bulk_upsert_daily_movement(docs)

    if verbose:
        print("Done.")
    return result


if __name__ == "__main__":
    try:
        res = run_once(verbose=True)
        if res:
            print(res)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
