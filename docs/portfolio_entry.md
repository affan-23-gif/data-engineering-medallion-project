# Upwork portfolio entry (publish only after running and checking the project)

**Title:** End-to-End ETL Pipeline with PySpark and Delta Lake

**Your role:** Data Engineer — Personal Project

**Description (within Upwork's portfolio description limit):**

Built a local end-to-end ETL pipeline using Python, PySpark, and Delta Lake. Raw customer, product, and order data pass through Bronze, Silver, and Gold layers. The pipeline normalizes types, removes duplicate keys, validates data, quarantines rejected records with reason codes, and creates monthly sales, customer summary, and product performance tables. Includes reproducible setup instructions, synthetic source data, quality checks, and automated input-data tests.

**Suggested skills:** Python, PySpark, SQL, ETL, Data Engineering

**Recommended portfolio attachments after you run it:**
1. `docs/architecture.png` — pipeline architecture.
2. Screenshot of the printed quality report from `python scripts/run_pipeline.py`.
3. Screenshot of the `monthly_sales` table from `python scripts/inspect_gold.py`.
4. GitHub repository URL, once you have pushed the project and verified that it runs.

**Transparency note:** This is a personal demo project built with synthetic data, not paid client work.
