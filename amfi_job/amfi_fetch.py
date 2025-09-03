import requests
from .config import Config


def fetch_nav_text(cfg: Config):
    resp = requests.get(cfg.amfi_nav_url, timeout=60)
    resp.raise_for_status()
    return resp.text
