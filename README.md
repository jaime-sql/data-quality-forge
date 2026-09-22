# data-quality-forge

Teaching scaffold for **Week 9 Lab 4 — data cleansing and profiling**. Students profile a small, intentionally messy orders batch in PySpark, then practice safe casting, quarantine (dead-letter) routing, and windowed deduplication. The notebook is a lab, not an answer key.

## Learning objectives

Each dimension is measured before it is “fixed”:

| Dimension | What you should be able to do |
| --- | --- |
| **Completitud** (completeness) | Tell a real value from null, blank text, and sentinels such as `N/A` or `null`. `isNotNull()` alone is not enough. |
| **Validez** (validity) | Parse dates, amounts, and quantities with `try_cast` or an explicit `when` / null. Reject silent `.cast` that turns garbage into null without a rule. Keep only values inside the lab domain (positive amount, quantity ≥ 1, known status). |
| **Unicidad** (uniqueness) | Collapse duplicate `order_id` values with `row_number` over a `Window.partitionBy`, and state which copy survives (latest `order_date`). |
| **Consistencia** (consistency) | Check cross-field agreement. In this lab, ISO-2 country and currency must match (`ES`/`FR`→EUR, `MX`→MXN, `US`→USD). |
| **Precisión** (accuracy) | Compare a parsed amount with its reference within an absolute tolerance of `0.01`. Missing numbers are not precise. |

## Layout

```
.github/workflows/deploy_databricks.yml   # pytest, then Databricks import
notebooks/Week09_Lab04_DataCleansing_and_Profiling.py
data_quality/validation.py                # pure-Python rules (no Spark)
tests/test_data_validation_rules.py
pyproject.toml                            # puts the repo root on pytest's path
requirements-dev.txt
```

`data_quality/validation.py` is the cluster-free contract for the same ideas (null / completeness, validity ranges, uniqueness, country-currency consistency, absolute tolerance). The Databricks notebook reimplements them in Spark behind `# TODO(estudiante):` prompts.

## Run the tests

Python 3.10 or newer. No Spark installation and no Databricks credentials are required.

```bash
python -m pip install -r requirements-dev.txt
pytest -q
```

## How CI deploys

Workflow: [`.github/workflows/deploy_databricks.yml`](.github/workflows/deploy_databricks.yml).

It runs on every **push to `main`** and when someone starts it with **workflow_dispatch**.

1. **Unit tests** — checks out the repo, sets up Python, installs pytest and `requirements-dev.txt`, then runs `pytest -q`.
2. **Import** — only if tests pass (`needs: test`). The job:
   - fails immediately when `DATABRICKS_HOST` or `DATABRICKS_TOKEN` is missing;
   - creates `/Shared/Week09_Lab04_DataCleansing` with the Workspace `mkdirs` API if it is not already there;
   - imports each `notebooks/*.py` file with the Workspace `import` API as **SOURCE / PYTHON**, with **overwrite**.

Notebooks land at:

```
/Shared/Week09_Lab04_DataCleansing/Week09_Lab04_DataCleansing_and_Profiling
```

`DATABRICKS_HOST` must be the workspace URL only, for example `https://adb-xxxx.azuredatabricks.net` or `https://adb-xxxx.cloud.databricks.com`, with no token in the value.

## Secrets

Store `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as GitHub Actions repository secrets (Settings → Secrets and variables → Actions). They are already expected on this repo.

Never commit tokens, `.env` files, or `.databrickscfg`. Do not paste a personal access token into the notebook, the workflow, or a pull request. The workflow reads the secrets from the Actions environment and does not print them.

## Work the lab

Open the imported notebook in Databricks and run it from the top. Setup and the synthetic batch are ready. Each later section raises `NotImplementedError` until you replace the `# TODO(estudiante):` with your own Spark code. When a row could reasonably be normalized or quarantined (European decimals, `USA` vs `US`, dates that use `/`), write the decision in a short comment next to the code.
