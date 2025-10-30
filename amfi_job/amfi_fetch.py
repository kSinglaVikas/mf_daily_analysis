from time import sleep
import requests
from .config import Config

class DataNotAvailableError(Exception):
    """Raised when data is not available for a specific date (404 error)"""
    pass

# Retry for 3 times with 2 minutes delay, but handle 404 specially
def fetch_nav_text(cfg: Config):
    for attempt in range(3):
        print(f"[DEBUG] Attempt {attempt + 1}...")
        try:
            resp = requests.get(cfg.amfi_nav_url, timeout=120)
            if resp.status_code == 404:
                print(f"[INFO] No data available for this date (404 error). Skipping...")
                raise DataNotAvailableError("No data available for this date")
            resp.raise_for_status()
            return resp.text  # Return text content for TXT file
        except DataNotAvailableError:
            # Don't retry for 404 errors, just re-raise
            raise
        except requests.RequestException as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e}")
            if attempt < 2:  # Don't sleep after the last attempt
                print("[DEBUG] Retrying in 1 minute...")
                sleep(60)  # Wait for 1 minute before retrying
    raise RuntimeError("Failed to fetch NAV text after 3 attempts")
