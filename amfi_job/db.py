from __future__ import annotations
from typing import Iterable, List, Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
from datetime import datetime

from .config import Config


class DB:
    def __init__(self, cfg: Config):
        self.client = MongoClient(cfg.mongodb_uri)
        self.db_reporting = self.client[cfg.db_reporting]
        self.db_mutual = self.client[cfg.db_mutualfunds]

    def get_active_schemes(self) -> List[Dict[str, Any]]:
        coll = self.db_reporting["mf_activeSchemes"]
        # Expect fields: scheme_code, amc_code, category, sub_category, etc.
        docs = list(coll.find({}, {"_id": 0}))
        return docs
    
    def get_latest_date_from_daily_movement(self) -> Optional[datetime]:
        """Get the latest date from the daily_movement collection"""
        coll = self.db_mutual["daily_movement"]
        # Find the document with the maximum Date field
        result = coll.find_one(
            {"Date": {"$ne": None}},  # Exclude null dates
            sort=[("Date", -1)]  # Sort by Date descending
        )
        if result and result.get("Date"):
            return result["Date"]
        return None

    def bulk_upsert_daily_movement(self, docs: Iterable[Dict[str, Any]]):
        coll = self.db_mutual["daily_movement"]
        ops = []
        for d in docs:
            key = {
                "Scheme Code": d.get("Scheme Code"),
                "Date": d.get("Date"),
            }
            ops.append(
                UpdateOne(key, {"$set": d}, upsert=True)
            )
        if ops:
            result = coll.bulk_write(ops, ordered=False)
            return result.bulk_api_result
        return {"nUpserted": 0, "nModified": 0}
