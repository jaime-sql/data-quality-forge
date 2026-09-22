# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Bronze → Quarantine → Silver
# MAGIC
# MAGIC Runnable CI / Job notebook (no student TODOs). Implements the same quality ideas as Lab 4
# MAGIC on a small synthetic bronze orders batch: profile → safe cast → quarantine with reason
# MAGIC codes → window dedupe → silver.
# MAGIC
# MAGIC En producción se escribirían tablas Delta; aquí todo queda en memoria para Free Edition / Job.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Dominios cerrados / closed domains (igual que Lab 4)
ALLOWED_STATUS = ("PAID", "SHIPPED", "CANCELLED")
COUNTRY_CURRENCY = {"ES": "EUR", "FR": "EUR", "MX": "MXN", "US": "USD"}

# Códigos de rechazo / reject reason codes
REJECT_MISSING_ORDER_ID = "completitud.order_id"
REJECT_AMOUNT = "validez.amount"
REJECT_QUANTITY = "validez.quantity"
REJECT_DATE = "validez.order_date"
REJECT_STATUS = "validez.status"
REJECT_COUNTRY_CURRENCY = "consistencia.country_currency"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — lotes sintéticos sucios / messy synthetic batch

# COMMAND ----------

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
    # Válida / valid reference
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "PAID"),
    # Duplicado exacto / exact duplicate
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "PAID"),
    # Misma clave, fecha más reciente → gana en dedupe / same key, later date wins
    ("A-100", "C-9", "2024-01-16", "ES", "EUR", "80.00", "1", "PAID"),
    # País/moneda inconsistentes / country-currency mismatch
    ("A-101", "C-2", "2024-02-01", "ES", "USD", "15.00", "1", "PAID"),
    # order_id ausente, fecha inválida, amount negativo, qty 0
    (None, "C-3", "2024-13-40", "MX", "MXN", "-5", "0", "CANCELLED"),
    # amount europeo + qty no numérico / European decimal + non-numeric qty
    ("A-102", "C-4", "not-a-date", "FR", "EUR", "1.200,50", "dos", "SHIPPED"),
    # Sentinelas N/A / null-like sentinels
    ("A-103", "C-5", "2024-03-01", "FR", "EUR", "N/A", "1", "PAID"),
    # qty negativo + status fuera de dominio / negative qty + unknown status
    ("A-104", "C-6", "2024-03-02", "US", "USD", "10.00", "-1", "UNKNOWN"),
    # Válida US/USD
    ("A-105", "C-7", "2024-03-03", "US", "USD", "99.99", "3", "SHIPPED"),
    # order_id en blanco / blank order_id
    ("", "C-8", "2024-03-04", "MX", "MXN", "50.00", "1", "PAID"),
    # Válida MX/MXN
    ("A-106", "C-9", "2024-03-05", "MX", "MXN", "250.00", "2", "PAID"),
    # amount cero → inválido en este dominio / zero amount invalid for domain
    ("A-107", "C-1", "2024-03-06", "MX", "MXN", "0", "1", "SHIPPED"),
]

bronze_orders = spark.createDataFrame(RAW_ROWS, schema=RAW_SCHEMA)
bronze_count = bronze_orders.count()
print(f"Bronze rows / filas bronze: {bronze_count}")
display(bronze_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Perfilado rápido / quick profile
# MAGIC Completitud (null-rate) y unicidad (duplicados por order_id).

# COMMAND ----------

total = float(bronze_count) if bronze_count else 1.0

null_rates = (
    bronze_orders.select(
        *[
            (F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)) / F.lit(total)).alias(
                f"null_rate_{c}"
            )
            for c in bronze_orders.columns
        ]
    )
)
print("Completeness / completitud (null or blank rate):")
display(null_rates)

dup_stats = (
    bronze_orders.filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != ""))
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)
duplicate_order_id_groups = dup_stats.count()
print(f"Duplicate order_id groups / grupos duplicados: {duplicate_order_id_groups}")
display(dup_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Casting seguro / safe casting
# MAGIC try_cast + when: basura → null controlado, no cast silencioso.

# COMMAND ----------

# Normaliza amount europeo "1.200,50" → "1200.50" cuando hay coma decimal
amount_normalized = (
    F.when(
        F.col("amount").rlike(r".*,\d+$"),
        F.regexp_replace(F.regexp_replace(F.col("amount"), r"\.", ""), ",", "."),
    ).otherwise(F.col("amount"))
)

# Safe cast via SQL try_cast (works on runtimes where F.try_cast is missing)
typed = (
    bronze_orders.withColumn("order_id_clean", F.trim(F.col("order_id")))
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
    .withColumn("order_date_ts", F.to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"))
    .withColumn("country_clean", F.upper(F.trim(F.col("country"))))
    .withColumn("currency_clean", F.upper(F.trim(F.col("currency"))))
    .withColumn("status_clean", F.upper(F.trim(F.col("status"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reglas → cuarentena / rules → quarantine with reason codes

# COMMAND ----------

# Mapa país→moneda como DataFrame para join (evita UDF)
cc_rows = [(k, v) for k, v in COUNTRY_CURRENCY.items()]
cc_df = spark.createDataFrame(cc_rows, ["country_clean", "expected_currency"])

with_rules = (
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
            F.when(
                ~F.col("status_clean").isin(*ALLOWED_STATUS),
                F.lit(REJECT_STATUS),
            ),
            F.when(
                F.col("expected_currency").isNull()
                | (F.col("currency_clean") != F.col("expected_currency")),
                F.lit(REJECT_COUNTRY_CURRENCY),
            ),
        ),
    )
    # Drop null slots from when() (Spark 3.1+ filter); avoids array_compact dependency
    .withColumn("reasons", F.expr("filter(_reason_arr, x -> x is not null)"))
    .withColumn("is_valid", F.size(F.col("reasons")) == 0)
)

quarantine = (
    with_rules.filter(~F.col("is_valid"))
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

valid_rows = with_rules.filter(F.col("is_valid"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dedupe por ventana / window dedupe — keep latest order_date per order_id

# COMMAND ----------

w = Window.partitionBy("order_id_clean").orderBy(F.col("order_date_ts").desc_nulls_last())

silver = (
    valid_rows.withColumn("rn", F.row_number().over(w))
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

# Producción: silver.write.format("delta").mode("overwrite").saveAsTable("...")
# Production would persist Delta bronze / quarantine / silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conteos / counts

# COMMAND ----------

quarantine_count = quarantine.count()
silver_count = silver.count()

print(f"Bronze:      {bronze_count}")
print(f"Quarantine:  {quarantine_count}")
print(f"Silver:      {silver_count}")

display(quarantine)
display(silver)

# Assert mínimos para que el Job falle en silencio roto / smoke asserts for CI Job
assert bronze_count > 0, "bronze must have rows"
assert quarantine_count > 0, "expected some quarantine rows from synthetic defects"
assert silver_count > 0, "expected some silver rows"
assert silver_count < bronze_count, "silver should be smaller after quarantine + dedupe"

print("Pipeline OK — bronze → quarantine → silver")
