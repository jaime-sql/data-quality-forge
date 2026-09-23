# Databricks Jobs (legacy)

CI now upserts a **Lakeflow / DLT Pipeline** from
[`databricks/pipelines/data_quality_pipeline.json`](../pipelines/data_quality_pipeline.json)
via `scripts/upsert_databricks_job.py` (name kept for workflow compatibility).

The notebook `Pipeline_Bronze_Quarantine_Silver` defines `@dlt.table` targets
(`bronze_orders`, `quarantine_orders`, `silver_orders`). Run it from **Pipelines**,
not as a classic notebook Job — classic Jobs do not register DLT tables and produce
`NO_TABLES_IN_PIPELINE` if you attach this notebook to a Pipeline without those decorators
(or the opposite: running a non-DLT notebook as a Pipeline).

Do **not** point the pipeline at the Lab 4 student notebook
(`Week09_Lab04_DataCleansing_and_Profiling`) — it raises `NotImplementedError` on purpose.
