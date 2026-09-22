# Databricks Jobs (upsert by name)

This folder holds Jobs API **2.1** definitions used by `scripts/upsert_databricks_job.py`.

## `data_quality_pipeline.json`

- **Job name:** `data-quality-forge-pipeline` (exact match used for upsert).
- **Task:** `bronze_to_silver` runs the workspace notebook
  `/Shared/Week09_Lab04_DataCleansing/Pipeline_Bronze_Quarantine_Silver`
  (imported by CI from `notebooks/Pipeline_Bronze_Quarantine_Silver.py`).
- **Do not** point the Job at the Lab 4 student notebook
  (`Week09_Lab04_DataCleansing_and_Profiling`) — it raises `NotImplementedError` on purpose.

### Compute / Free Edition

The JSON omits `new_cluster` and `job_clusters` so Databricks can attach **serverless** or
workspace-default compute (including Free Edition). If your workspace requires an explicit
cluster, add a `job_clusters` / `job_cluster_key` block locally or via the UI; keep the
`notebook_path` unchanged.

### Upsert behaviour

`scripts/upsert_databricks_job.py`:

1. `GET /api/2.1/jobs/list` (paginated) and find a job whose **name** equals
   `data-quality-forge-pipeline`.
2. If found → `POST /api/2.1/jobs/reset` with `{ "job_id", "new_settings": <JSON> }`.
3. If not → `POST /api/2.1/jobs/create` with the JSON body as-is.
4. If `RUN_JOB=true` (CI default) → `POST /api/2.1/jobs/run-now` and print the `run_id`.

Requires GitHub Actions secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Never commit tokens.
