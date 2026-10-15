# Silver layer package

Copy these into the repo (same paths), commit on a feature branch, open a PR.

| File | Purpose |
|---|---|
| src/common/mappings.py | Reference mappings and clinical limits |
| src/common/silver_transforms.py | Cleaning and standardisation functions |
| src/common/dq_checks.py | Reusable rule engine, quarantine split, results log |
| src/common/silver_rules.py | Validation rules for the clinical trial table |
| src/silver/clean_clinical_trial.py | Silver notebook (Databricks job task) |
| src/silver/dq_silver.py | Post-load quality checks (Databricks job task) |
| tests/ | 97 pytest tests (run with PySpark) |
| resources/jobs.yml | Job: bronze, bronze_dq, silver, silver_dq |
| .github/workflows/ci.yml | CI with Java and PySpark |
| requirements.txt | pytest, pyspark |

Tables created on the first Silver run: silver.clinical_trial, silver.clinical_trial_quarantine,
silver.dq_results, silver.etl_control.
