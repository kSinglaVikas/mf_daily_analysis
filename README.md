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
- pyproject.toml
- uv.lock
- .env (not committed)


Quick start
1) Create .env with MongoDB connection string and optional overrides
2) Install `uv` if it is not already installed:
  ```bash
  brew install uv
  ```
3) Create the environment and install the locked dependencies:
   ```bash
  uv sync
   ```

How to execute jobs

- To fetch and upsert latest AMFI NAV data:
  ```bash
  uv run python -m amfi_job.job
  ```

- To print a category/date value table from the database:
  ```bash
  uv run python -m amfi_job.report_table
  ```


Environment variables
- MONGODB_URI: mongodb connection string (mongodb+srv:// or mongodb://)
- MONGODB_DB_REPORTING: defaults to reporting
- MONGODB_DB_MUTUALFUNDS: defaults to mutualFunds
- AMFI_NAV_URL: override AMFI URL

