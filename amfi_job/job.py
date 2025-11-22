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
        print(f"Done processing date: {date_str}")
    return result


def _determine_start_date(latest_date: Optional[datetime], yesterday: datetime, verbose: bool) -> datetime:
    """Determine the start date for processing based on latest DB date"""
    if latest_date is None:
        if verbose:
            print("No existing data found in database. Starting from yesterday.")
        return yesterday
    
    start_date = latest_date + timedelta(days=1)
    if verbose:
        print(f"Latest date in database: {latest_date.strftime('%Y-%m-%d')}")
        print(f"Starting from: {start_date.strftime('%Y-%m-%d')}")
    return start_date


def _process_single_date(date_str: str, verbose: bool) -> Optional[dict]:
    """Process a single date and return the result"""
    try:
        if verbose:
            print(f"\n--- Processing date: {date_str} ---")
        result = run_once_for_date(date_str, verbose)
        if result:
            return {"date": date_str, "result": result}
    except DataNotAvailableError:
        if verbose:
            print(f"No data available for date {date_str}. Skipping to next date.")
    except Exception as e:
        print(f"Error processing date {date_str}: {e}")
    return None


def _process_date_range(start_date: datetime, yesterday: datetime, verbose: bool) -> list:
    """Process all dates from start_date to yesterday"""
    if verbose:
        print(f"Will process dates from {start_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')} (inclusive)")
    
    total_results = []
    current_date = start_date
    
    while current_date <= yesterday:
        date_str = current_date.strftime("%Y-%m-%d")
        result = _process_single_date(date_str, verbose)
        if result:
            total_results.append(result)
        current_date += timedelta(days=1)
    
    if verbose:
        print(f"\n--- Completed processing. Processed {len(total_results)} dates ---")
    
    return total_results


def run_once(verbose: bool = True) -> Optional[dict]:
    """Run the job from latest date in DB until yesterday"""
    cfg = Config.from_env()
    db = DB(cfg)
    
    latest_date = db.get_latest_date_from_daily_movement()
    yesterday = datetime.now() - timedelta(days=1)
    
    if verbose:
        print(f"Today: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"Yesterday: {yesterday.strftime('%Y-%m-%d')}")
    
    start_date = _determine_start_date(latest_date, yesterday, verbose)
    
    if start_date > yesterday:
        if verbose:
            print("Database is already up to date. No processing needed.")
        return {"message": "Database is up to date"}
    
    total_results = _process_date_range(start_date, yesterday, verbose)
    
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
