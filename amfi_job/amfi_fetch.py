from time import sleep
import requests
from .config import Config

# Retry for 3 times with 2 minutes delay
def fetch_nav_text(cfg: Config):
    for attempt in range(3):
        print(f"[DEBUG] Attempt {attempt + 1}...")
        try:
            resp = requests.get(cfg.amfi_nav_url, timeout=120)
            resp.raise_for_status()
            return resp.content  # Return bytes for Excel file
        except requests.RequestException as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e}")
            print("[DEBUG] Retrying in 1 minute...")
            sleep(60)  # Wait for 1 minute before retrying
    raise RuntimeError("Failed to fetch NAV text after 3 attempts")
