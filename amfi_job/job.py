from __future__ import annotations
import sys
from typing import Optional
from datetime import datetime, timedelta

from .config import Config
from .amfi_fetch import fetch_nav_text, DataNotAvailableError
from .amfi_parse import parse_nav_text, minimal_nav
from .db import DB
from .merge import merge_nav_with_active, to_daily_movement_docs


def run_once_for_date(date_str: str, verbose: bool = True) -> Optional[dict]:
    """Run the job for a specific date"""
    cfg = Config.from_env().with_date(date_str)

    if verbose:
        print(f"Fetching AMFI NAV file for date: {date_str}")
    
    try:
        text = fetch_nav_text(cfg)
    except DataNotAvailableError:
        # Re-raise so the caller can handle it
        raise

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
        print(f"Done processing date: {date_str}")
    return result


def run_once(verbose: bool = True) -> Optional[dict]:
    """Run the job from latest date in DB until yesterday"""
    cfg = Config.from_env()
    db = DB(cfg)
    
    # Get the latest date from the database
    latest_date = db.get_latest_date_from_daily_movement()
    yesterday = datetime.now() - timedelta(days=1)
    
    if verbose:
        print(f"Today: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"Yesterday: {yesterday.strftime('%Y-%m-%d')}")
    
    if latest_date is None:
        if verbose:
            print("No existing data found in database. Starting from yesterday.")
        start_date = yesterday
    else:
        # Start from the day after the latest date in DB
        start_date = latest_date + timedelta(days=1)
        if verbose:
            print(f"Latest date in database: {latest_date.strftime('%Y-%m-%d')}")
            print(f"Starting from: {start_date.strftime('%Y-%m-%d')}")
    
    if start_date > yesterday:
        if verbose:
            print("Database is already up to date. No processing needed.")
        return {"message": "Database is up to date"}
    
    # Process each date from start_date to yesterday
    current_date = start_date
    total_results = []
    
    if verbose:
        print(f"Will process dates from {start_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')} (inclusive)")
    
    while current_date <= yesterday:
        date_str = current_date.strftime("%Y-%m-%d")
        
        try:
            if verbose:
                print(f"\n--- Processing date: {date_str} ---")
                print(f"Current date: {current_date.strftime('%Y-%m-%d')}, Yesterday: {yesterday.strftime('%Y-%m-%d')}")
            result = run_once_for_date(date_str, verbose)
            if result:
                total_results.append({
                    "date": date_str,
                    "result": result
                })
        except DataNotAvailableError:
            if verbose:
                print(f"No data available for date {date_str}. Skipping to next date.")
            # Continue with next date
        except Exception as e:
            print(f"Error processing date {date_str}: {e}")
            # Continue with next date instead of stopping
        
        current_date += timedelta(days=1)
        if verbose:
            print(f"Next date will be: {current_date.strftime('%Y-%m-%d')}")
    
    if verbose:
        print(f"\n--- Completed processing. Processed {len(total_results)} dates ---")
    
    return {
        "processed_dates": len(total_results),
        "results": total_results
    }


if __name__ == "__main__":
    try:
        res = run_once(verbose=False)
        if res:
            print(res)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
