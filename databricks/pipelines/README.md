# Databricks Pipelines (Lakeflow / DLT)

## `data_quality_pipeline.json`

- **Name:** `data-quality-forge-pipeline`
- **Notebook:** `/Shared/Week09_Lab04_DataCleansing/Pipeline_Bronze_Quarantine_Silver`
- **Tables:** `bronze_orders`, `quarantine_orders`, `silver_orders` (plus view `orders_typed`)
- **Defaults:** Unity Catalog `workspace.data_quality_forge`, serverless, development mode

Override catalog/schema with env `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA` in CI if your
Free Edition workspace uses different names.

### Bronze source

`bronze_orders` reads the class-demo CSV from the Unity Catalog Volume
`/Volumes/workspace/data_quality_forge/landing/orders/orders_messy.csv`.
The same 12 messy lab rows are in `data/landing/orders/orders_messy.csv`.
Bronze is no longer purely synthetic: the Volume file is the source when it is present.
If that file is missing, the notebook falls back to the in-notebook `RAW_ROWS` batch.
Quarantine and silver logic are unchanged.

### How to run in the UI

1. Import / open the notebook under `/Shared/Week09_Lab04_DataCleansing/`.
2. **Pipelines → Create pipeline** (or open the existing `data-quality-forge-pipeline`).
3. Source: this notebook. Target catalog/schema as above.
4. Put `orders_messy.csv` on the Volume path above (or rely on `RAW_ROWS` if it is absent), then **Start**.

If you see `NO_TABLES_IN_PIPELINE`, the Pipeline is attached to a notebook that has no
`@dlt.table` definitions — use this notebook, not the Lab 4 TODO notebook.
