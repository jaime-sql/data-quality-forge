# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Bronze → Quarantine → Silver (Delta Live Tables / Lakeflow)
# MAGIC
# MAGIC This notebook is meant to run as a **Databricks Pipeline** (ETL / DLT), not as a
# MAGIC one-off "Run all" Job cell notebook. Each `@dlt.table` becomes a pipeline table.
# MAGIC
# MAGIC Same Lab 4 ideas on a synthetic bronze orders batch: profile helpers → safe cast →
# MAGIC quarantine with reason codes → window dedupe → silver.
# MAGIC
# MAGIC In the UI: **Pipelines → Create pipeline →** attach this notebook path, then **Start**.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Dominios cerrados / closed domains (same as Lab 4)
ALLOWED_STATUS = ("PAID", "SHIPPED", "CANCELLED")
COUNTRY_CURRENCY = {"ES": "EUR", "FR": "EUR", "MX": "MXN", "US": "USD"}

# Códigos de rechazo / reject reason codes
REJECT_MISSING_ORDER_ID = "completitud.order_id"
REJECT_AMOUNT = "validez.amount"
REJECT_QUANTITY = "validez.quantity"
REJECT_DATE = "validez.order_date"
REJECT_STATUS = "validez.status"
REJECT_COUNTRY_CURRENCY = "consistencia.country_currency"

RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("country", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("qty", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# Comentario por fila = defecto a descubrir / defect each row demonstrates
RAW_ROWS = [
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "PAID"),
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "PAID"),
    ("A-100", "C-9", "2024-01-16", "ES", "EUR", "80.00", "1", "PAID"),
    ("A-101", "C-2", "2024-02-01", "ES", "USD", "15.00", "1", "PAID"),
    (None, "C-3", "2024-13-40", "MX", "MXN", "-5", "0", "CANCELLED"),
    ("A-102", "C-4", "not-a-date", "FR", "EUR", "1.200,50", "dos", "SHIPPED"),
    ("A-103", "C-5", "2024-03-01", "FR", "EUR", "N/A", "1", "PAID"),
    ("A-104", "C-6", "2024-03-02", "US", "USD", "10.00", "-1", "UNKNOWN"),
    ("A-105", "C-7", "2024-03-03", "US", "USD", "99.99", "3", "SHIPPED"),
    ("", "C-8", "2024-03-04", "MX", "MXN", "50.00", "1", "PAID"),
    ("A-106", "C-9", "2024-03-05", "MX", "MXN", "250.00", "2", "PAID"),
    ("A-107", "C-1", "2024-03-06", "MX", "MXN", "0", "1", "SHIPPED"),
]


def _typed_frame(bronze):
    """Safe casts: garbage → controlled null (try_cast / try_to_date)."""
    amount_normalized = F.when(
        F.col("amount").rlike(r".*,\d+$"),
        F.regexp_replace(F.regexp_replace(F.col("amount"), r"\.", ""), ",", "."),
    ).otherwise(F.col("amount"))

    typed = (
        bronze.withColumn("order_id_clean", F.trim(F.col("order_id")))
        .withColumn("amount_norm", amount_normalized)
        .withColumn(
            "amount_num",
            F.when(
                F.upper(F.trim(F.col("amount_norm"))).isin("N/A", "NULL", "NONE", ""),
                F.lit(None).cast(DoubleType()),
            ).otherwise(F.expr("try_cast(amount_norm AS DOUBLE)")),
        )
        .withColumn("qty_trim", F.trim(F.col("qty")))
        .withColumn("qty_num", F.expr("try_cast(qty_trim AS INT)"))
        .withColumn("order_date_raw", F.trim(F.col("order_date")))
        .withColumn("order_date_ts", F.expr("try_to_date(order_date_raw, 'yyyy-MM-dd')"))
        .withColumn("country_clean", F.upper(F.trim(F.col("country"))))
        .withColumn("currency_clean", F.upper(F.trim(F.col("currency"))))
        .withColumn("status_clean", F.upper(F.trim(F.col("status"))))
    )

    cc_df = spark.createDataFrame(
        [(k, v) for k, v in COUNTRY_CURRENCY.items()],
        ["country_clean", "expected_currency"],
    )

    return (
        typed.join(cc_df, on="country_clean", how="left")
        .withColumn(
            "_reason_arr",
            F.array(
                F.when(
                    F.col("order_id_clean").isNull() | (F.col("order_id_clean") == ""),
                    F.lit(REJECT_MISSING_ORDER_ID),
                ),
                F.when(
                    F.col("amount_num").isNull() | (F.col("amount_num") <= 0),
                    F.lit(REJECT_AMOUNT),
                ),
                F.when(
                    F.col("qty_num").isNull() | (F.col("qty_num") < 1),
                    F.lit(REJECT_QUANTITY),
                ),
                F.when(F.col("order_date_ts").isNull(), F.lit(REJECT_DATE)),
                F.when(~F.col("status_clean").isin(*ALLOWED_STATUS), F.lit(REJECT_STATUS)),
                F.when(
                    F.col("expected_currency").isNull()
                    | (F.col("currency_clean") != F.col("expected_currency")),
                    F.lit(REJECT_COUNTRY_CURRENCY),
                ),
            ),
        )
        .withColumn("reasons", F.expr("filter(_reason_arr, x -> x is not null)"))
        .withColumn("is_valid", F.size(F.col("reasons")) == 0)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — synthetic messy batch (pipeline table)

# COMMAND ----------


@dlt.table(
    name="bronze_orders",
    comment="Synthetic messy orders batch (bronze). Teaching source for Lab 4 quality rules.",
)
def bronze_orders():
    return spark.createDataFrame(RAW_ROWS, schema=RAW_SCHEMA)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Typed + rules (view) — safe cast and reject reasons

# COMMAND ----------


@dlt.view(name="orders_typed")
def orders_typed():
    return _typed_frame(dlt.read("bronze_orders"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quarantine — invalid rows with reason codes (pipeline table)

# COMMAND ----------


@dlt.table(
    name="quarantine_orders",
    comment="Rows that failed completeness / validity / consistency checks, with reject_reasons.",
)
def quarantine_orders():
    return (
        dlt.read("orders_typed")
        .filter(~F.col("is_valid"))
        .withColumn("reject_reasons", F.concat_ws(",", F.col("reasons")))
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "country",
            "currency",
            "amount",
            "qty",
            "status",
            "amount_num",
            "qty_num",
            "order_date_ts",
            "reject_reasons",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — valid rows, latest order_date per order_id (pipeline table)

# COMMAND ----------


@dlt.table(
    name="silver_orders",
    comment="Valid orders after quarantine filters and windowed dedupe on order_id.",
)
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_drop("qty_at_least_one", "qty >= 1")
def silver_orders():
    w = Window.partitionBy("order_id_clean").orderBy(F.col("order_date_ts").desc_nulls_last())
    return (
        dlt.read("orders_typed")
        .filter(F.col("is_valid"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("order_id_clean").alias("order_id"),
            F.col("customer_id"),
            F.col("order_date_ts").alias("order_date"),
            F.col("country_clean").alias("country"),
            F.col("currency_clean").alias("currency"),
            F.col("amount_num").alias("amount"),
            F.col("qty_num").alias("qty"),
            F.col("status_clean").alias("status"),
        )
    )
