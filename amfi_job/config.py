from datetime import datetime, timedelta
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env if present at repo root
load_dotenv()

def get_amfi_url_for_date(date_str: str) -> str:
    """Generate AMFI URL for a specific date"""
    return f"https://www.amfiindia.com/api/download-nav-history?strMFID=all&schemeTypeDesc=all&FromDate={date_str}&ToDate={date_str}"

@dataclass(frozen=True)
class Config:
    mongodb_uri: str
    db_reporting: str = os.environ.get("MONGODB_DB_REPORTING", "reporting")
    db_mutualfunds: str = os.environ.get("MONGODB_DB_MUTUALFUNDS", "mutualFunds")
    amfi_nav_url: str = ""  # Will be set dynamically per date

    @staticmethod
    def from_env() -> "Config":
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI env var is required")
        return Config(mongodb_uri=uri)
    
    def with_date(self, date_str: str) -> "Config":
        """Create a new Config instance with AMFI URL for the specified date"""
        return Config(
            mongodb_uri=self.mongodb_uri,
            db_reporting=self.db_reporting,
            db_mutualfunds=self.db_mutualfunds,
            amfi_nav_url=get_amfi_url_for_date(date_str)
        )
