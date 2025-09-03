AMFI Daily NAV to MongoDB Job

Overview
- Fetch latest AMFI NAV text file
- Parse into structured rows
- Merge with MongoDB collection reporting.mf_activeSchemes
- Upsert merged records into mutualFunds.daily_movement

Structure
- amfi_job/
  - __init__.py
  - config.py
  - amfi_fetch.py
  - amfi_parse.py
  - db.py
  - merge.py
  - job.py
  - utils.py
- requirements.txt
- .env (not committed)

Quick start
1) Create .env with MongoDB connection string and optional overrides
2) Install deps
3) Run the job

Environment variables
- MONGODB_URI: mongodb connection string (mongodb+srv:// or mongodb://)
- MONGODB_DB_REPORTING: defaults to reporting
- MONGODB_DB_MUTUALFUNDS: defaults to mutualFunds
- AMFI_NAV_URL: override AMFI URL

