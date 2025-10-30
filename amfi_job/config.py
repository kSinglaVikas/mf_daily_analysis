from datetime import datetime, timedelta
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env if present at repo root
load_dotenv()

def convert_date_format(date_str: str) -> str:
    """Convert date from YYYY-MM-DD to DD-MMM-YYYY format
    
    Args:
        date_str: Date in YYYY-MM-DD format (e.g., "2025-10-28")
    
    Returns:
        Date in DD-MMM-YYYY format (e.g., "28-Oct-2025")
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%d-%b-%Y")
    except ValueError:
        # If already in DD-MMM-YYYY format, return as is
        return date_str

def get_amfi_url_for_date(date_str: str) -> str:
    """Generate AMFI URL for a specific date
    
    Args:
        date_str: Date in YYYY-MM-DD format (e.g., "2025-10-28")
    
    Returns:
        URL string for AMFI portal download
    """
    # Convert to DD-MMM-YYYY format required by the portal
    amfi_date_str = convert_date_format(date_str)
    return f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={amfi_date_str}"

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
