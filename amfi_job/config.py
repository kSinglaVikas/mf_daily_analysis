from datetime import datetime, timedelta
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env if present at repo root
load_dotenv()

# Use previous day's date to ensure data availability
# Change the date as needed or make it dynamic

yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
# Override for testing purposes
#yesterday_str = "2025-09-18"

print(f"Using AMFI data for date: {yesterday_str}")

DEFAULT_AMFI_URL = f"https://www.amfiindia.com/api/download-nav-history?strMFID=all&schemeTypeDesc=all&FromDate={yesterday_str}&ToDate={yesterday_str}"

@dataclass(frozen=True)
class Config:
    mongodb_uri: str
    db_reporting: str = os.environ.get("MONGODB_DB_REPORTING", "reporting")
    db_mutualfunds: str = os.environ.get("MONGODB_DB_MUTUALFUNDS", "mutualFunds")
    amfi_nav_url: str = os.environ.get("AMFI_NAV_URL", DEFAULT_AMFI_URL)

    @staticmethod
    def from_env() -> "Config":
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI env var is required")
        return Config(mongodb_uri=uri)
