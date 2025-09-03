import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env if present at repo root
load_dotenv()

DEFAULT_AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

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
