# Databricks notebook source
# MAGIC %md
# MAGIC # Semana 9 — Lab 4: Limpieza y perfilado de datos
# MAGIC
# MAGIC Scaffold de tutoría para **Data Architecture / DataOps**. Este notebook **no es una clave de respuestas**: cada técnica queda como `# TODO(estudiante):` y lanza `NotImplementedError` hasta que la implementes.
# MAGIC
# MAGIC Trabajas cinco dimensiones sobre un lote sintético y sucio:
# MAGIC
# MAGIC | Dimensión | Pregunta |
# MAGIC | --- | --- |
# MAGIC | **Completitud** | ¿El valor está presente o es un nulo disfrazado (`""`, `N/A`, `null`)? |
# MAGIC | **Validez** | ¿Se puede interpretar y cae en un dominio permitido? |
# MAGIC | **Unicidad** | ¿La clave de negocio aparece una sola vez? |
# MAGIC | **Consistencia** | ¿Los campos relacionados cuentan la misma historia? |
# MAGIC | **Precisión** | ¿El número está cerca del valor de referencia? |
# MAGIC
# MAGIC Reglas de oro del lab:
# MAGIC
# MAGIC - No hagas un `cast` silencioso. Un valor ilegible debe quedar en **null controlado** (`try_cast` / `when`) y la fila debe poder ir a **cuarentena**.
# MAGIC - No dedupliques con `distinct()` a ciegas: usa una ventana `row_number` para justificar qué copia sobrevive.
# MAGIC - No pegues tokens, hosts ni secretos en el notebook. En Databricks el `SparkSession` ya existe como `spark`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Cómo usar este lab
# MAGIC
# MAGIC 1. Ejecuta las celdas de setup y de datos sucios. Esas sí están completas.
# MAGIC 2. En cada celda de ejercicio, sustituye el `raise NotImplementedError(...)` por tu transformación y vuelve a ejecutar.
# MAGIC 3. Conserva las columnas crudas (`*_raw`). Las columnas limpias son nuevas.
# MAGIC 4. El contrato que debe cumplir tu resultado está en la sección final. Si una regla de negocio es ambigua, documenta la decisión en un comentario encima del código.
# MAGIC
# MAGIC Las mismas ideas, en Python puro y sin clúster, están en `data_quality/validation.py` y se comprueban con `pytest`. No importes ese módulo aquí: en el workspace solo se sincroniza `notebooks/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType

# `spark` lo inyecta el runtime de Databricks.
# Funciones que vas a necesitar más adelante (esto no resuelve los TODOs):
#   F.try_cast, F.when, F.trim, F.lower, F.upper, F.col, F.lit, F.abs
#   F.array, F.row_number, Window.partitionBy, Window.orderBy

# COMMAND ----------

# Dominios que el lab considera cerrados. Úsalos; no los redefinas a medias.
ALLOWED_STATUS = ("PAID", "SHIPPED", "CANCELLED")
COUNTRY_CURRENCY = {"ES": "EUR", "FR": "EUR", "MX": "MXN", "US": "USD"}
PRECISION_TOLERANCE = 0.01

# Códigos de motivo para la cuarentena. Añade uno por regla que falle.
REJECT_MISSING_ORDER_ID = "completitud.order_id"
REJECT_AMOUNT = "validez.amount"
REJECT_QUANTITY = "validez.quantity"
REJECT_DATE = "validez.order_date"
REJECT_EMAIL = "validez.email"
REJECT_STATUS = "validez.status"
REJECT_COUNTRY_CURRENCY = "consistencia.country_currency"
REJECT_PRECISION = "precision.amount"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos sucios
# MAGIC
# MAGIC Todo llega como texto, igual que un fichero mal tipado. El comentario de cada fila es el defecto que tienes que descubrir con código, no a ojo.

# COMMAND ----------

RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date_raw", StringType(), True),
        StructField("country_raw", StringType(), True),
        StructField("currency_raw", StringType(), True),
        StructField("amount_raw", StringType(), True),
        StructField("quantity_raw", StringType(), True),
        StructField("email_raw", StringType(), True),
        StructField("status_raw", StringType(), True),
        StructField("expected_amount_raw", StringType(), True),
    ]
)

# order_id, customer_id, order_date_raw, country_raw, currency_raw,
# amount_raw, quantity_raw, email_raw, status_raw, expected_amount_raw
RAW_ROWS = [
    # Fila válida de referencia.
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "ana@example.com", "PAID", "120.50"),
    # Duplicado exacto de la anterior.
    ("A-100", "C-1", "2024-01-15", "ES", "EUR", "120.50", "2", "ana@example.com", "PAID", "120.50"),
    # Misma clave, otro día y otro importe: hay que elegir una superviviente.
    ("A-100", "C-9", "2024-01-16", "ES", "EUR", "80.00", "1", "otro@example.com", "PAID", "80.00"),
    # País y moneda no cuentan la misma historia.
    ("A-101", "C-2", "2024-02-01", "ES", "USD", "15.00", "1", "luis@example.com", "PAID", "15.00"),
    # Identificador ausente, fecha imposible, importe negativo, cantidad 0, email en blanco.
    (None, "C-3", "2024-13-40", "MX", "MXN", "-5", "0", "", "CANCELLED", "-5"),
    # Importe con formato europeo y cantidad que no es un número. Cliente ausente.
    ("A-102", None, "not-a-date", "FR", "EUR", "1.200,50", "dos", "cara@example.com", "SHIPPED", "1200.50"),
    # Sentinelas de nulo en importe y email.
    ("A-103", "C-4", "2024-03-01", "FR", "EUR", "N/A", "1", "NULL", "PAID", "10.00"),
    # Alias de país, cantidad negativa y estado fuera de dominio.
    ("A-104", "C-5", "2024-03-02", "USA", "EUR", "10", "-1", "dani@example.com", "UNKNOWN", "10.00"),
    # Fecha con otro separador y estado en minúsculas. Decisión tuya: normalizar o cuarentena.
    ("A-105", "C-6", "2024/03/03", "MX", "MXN", "250.00", "3", "maria@example.com", "paid", "250.00"),
    # Identificador en blanco, país/moneda en minúsculas, importe lejos de la referencia.
    ("", "C-7", "2024-03-04", "es", "eur", "20.50", "1", "eva@example.com", "PAID", "19.00"),
    # Diferencia de 0.004: dentro de la tolerancia 0.01. Debe poder sobrevivir.
    ("A-106", "C-8", "2024-03-05", "US", "USD", "10.004", "1", "ian@example.com", "PAID", "10.00"),
    # Importe cero: presente, pero no válido para este dominio.
    ("A-107", "C-1", "2024-03-06", "MX", "MXN", "0", "1", "ana@example.com", "SHIPPED", "0"),
]

raw_orders = spark.createDataFrame(RAW_ROWS, schema=RAW_SCHEMA)

# COMMAND ----------

print("Filas ingestadas:", raw_orders.count())
raw_orders.printSchema()
display(raw_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Perfilado por dimensión
# MAGIC
# MAGIC Antes de limpiar, mide. Una métrica de perfil no arregla la fila: dice cuánto duele.
# MAGIC
# MAGIC Para Completitud no basta `isNotNull()`. La cadena `"N/A"` no es un valor de negocio.

# COMMAND ----------

def profile_completeness(df):
    """Devuelve una fila con el ratio de Completitud (0..1) de cada columna.

    Trata como ausente: null, blanco, y los tokens n/a, na, null, none, nan, nil, -.
    """
    # TODO(estudiante): recorre df.columns y agrega un ratio por columna.
    # Pista de forma (no es la métrica): F.when(condicion, 1).otherwise(0) y luego avg.
    raise NotImplementedError("TODO(estudiante): perfil de completitud por columna")


completeness_profile = profile_completeness(raw_orders)
display(completeness_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validez y cast seguro
# MAGIC
# MAGIC Anti-patrón: `F.col("amount_raw").cast("double")` convierte `"abc"` en null **sin avisar** y, según el origen, puede aceptar texto que no querías. En este lab el cast seguro es explícito.
# MAGIC
# MAGIC Firma útil, todavía sin aplicar a tus columnas:
# MAGIC
# MAGIC ```python
# MAGIC F.try_cast(F.lit("10"), "double")    # 10.0
# MAGIC F.try_cast(F.lit("abc"), "double")   # null
# MAGIC ```
# MAGIC
# MAGIC `"1.200,50"` no es un decimal plano. `try_cast` no adivina el formato europeo: o lo normalizas tú (y dejas escrito el criterio) o la fila va a cuarentena. No implementes las dos cosas en silencio.

# COMMAND ----------

def with_safe_casts(df):
    """Añade columnas tipadas sin borrar las crudas.

    Columnas nuevas esperadas:
    - order_date: date, o null si no es interpretable
    - amount: double, o null
    - quantity: int, o null
    - email: string recortado, o null si está ausente
    - status: string en mayúsculas, todavía sin filtrar el dominio
    - country: string en mayúsculas
    - currency: string en mayúsculas
    """
    # TODO(estudiante): usa F.try_cast / F.when(...).otherwise(F.lit(None)).
    # No filtres aquí. El enrutado a cuarentena es el paso 6.
    raise NotImplementedError("TODO(estudiante): cast seguro de importe, cantidad y fecha")


typed_orders = with_safe_casts(raw_orders)
display(typed_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Consistencia y precisión
# MAGIC
# MAGIC **Consistencia.** Tras normalizar mayúsculas, `country` y `currency` tienen que coincidir con `COUNTRY_CURRENCY`. `USA` no es una clave del mapa: decidir si la aliasas a `US` forma parte del ejercicio; si no la aliasas, esa fila no es consistente.
# MAGIC
# MAGIC **Precisión.** `amount` es preciso cuando `abs(amount - expected_amount) <= PRECISION_TOLERANCE` (0.01). Si alguno de los dos no se pudo parsear, la fila **no** es precisa: no rellenes el hueco con cero.

# COMMAND ----------

def with_consistency_flag(df):
    """Añade `is_consistent` (boolean) comparando país y moneda ya normalizados."""
    # TODO(estudiante): un when cruzado entre country y currency.
    # No uses una UDF: el mapa cabe en un when / create_map.
    raise NotImplementedError("TODO(estudiante): flag de consistencia país-moneda")


def with_precision_flag(df):
    """Añade `is_precise` (boolean) con tolerancia absoluta PRECISION_TOLERANCE."""
    # TODO(estudiante): F.abs sobre los dos importes ya parseados.
    # Si expected_amount no existe todavía, parsealo con el mismo criterio que amount.
    raise NotImplementedError("TODO(estudiante): flag de precisión contra expected_amount")


checked_orders = with_precision_flag(with_consistency_flag(typed_orders))
display(checked_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cuarentena (dead letter)
# MAGIC
# MAGIC Separa el lote en dos DataFrames:
# MAGIC
# MAGIC - `clean_orders`: filas que pasan **todas** las reglas de abajo.
# MAGIC - `quarantine_orders`: el resto, con la columna `reject_reasons` (`array<string>`) usando los códigos `REJECT_*`. Una fila puede acumular varios motivos.
# MAGIC
# MAGIC Reglas mínimas (puedes añadir, no quitar):
# MAGIC
# MAGIC - `order_id` utilizable (no null ni blanco) → si no, `REJECT_MISSING_ORDER_ID`
# MAGIC - `amount` parseado y `> 0` → si no, `REJECT_AMOUNT`
# MAGIC - `quantity` parseado y `>= 1` → si no, `REJECT_QUANTITY`
# MAGIC - `order_date` parseado → si no, `REJECT_DATE`
# MAGIC - email presente y con `@` → si no, `REJECT_EMAIL`
# MAGIC - `status` en `ALLOWED_STATUS` → si no, `REJECT_STATUS`
# MAGIC - país/moneda consistentes → si no, `REJECT_COUNTRY_CURRENCY`
# MAGIC - precisión dentro de tolerancia → si no, `REJECT_PRECISION`
# MAGIC
# MAGIC No borres filas del mundo: o están en `clean_orders` o están en `quarantine_orders`. La suma de conteos debe ser igual a `checked_orders.count()` **antes** de deduplicar.

# COMMAND ----------

def split_quarantine(df):
    """Devuelve `(clean_orders, quarantine_orders)`.

    `quarantine_orders` incluye las columnas de entrada más `reject_reasons`.
    `clean_orders` son las filas con cero motivos.
    """
    # TODO(estudiante): construye el array de motivos y filtra por su tamaño.
    # Pista de API: F.array, F.array_remove o F.filter, F.size. No hace falta un UDF.
    raise NotImplementedError("TODO(estudiante): enrutar filas inválidas a cuarentena")


clean_orders, quarantine_orders = split_quarantine(checked_orders)
display(quarantine_orders)
display(clean_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Unicidad con ventana
# MAGIC
# MAGIC Entre las filas **ya válidas**, quédate con una sola fila por `order_id`.
# MAGIC
# MAGIC Regla de negocio de este lab: sobrevive la de `order_date` más reciente. Si la fecha empata, documenta un desempate estable (por ejemplo `customer_id` descendente) y aplícalo en el `orderBy` de la ventana.
# MAGIC
# MAGIC No uses `dropDuplicates()` como única respuesta: no deja ver qué copia ganó.
# MAGIC
# MAGIC Firma de la ventana (todavía sin el filtro):
# MAGIC
# MAGIC ```python
# MAGIC window = Window.partitionBy("order_id").orderBy(...)
# MAGIC F.row_number().over(window)
# MAGIC ```
# MAGIC
# MAGIC Quédate con `row_number == 1`. Guarda el duplicado descartado si quieres auditarlo; no es obligatorio, pero anota cuántas filas eliminó la ventana.

# COMMAND ----------

def dedupe_orders(df):
    """Devuelve una fila por `order_id` usando `row_number` sobre una ventana."""
    # TODO(estudiante): Window.partitionBy + orderBy + row_number, luego filtra rn = 1.
    # No dedupliques la cuarentena: allí queremos conservar cada intento fallido.
    raise NotImplementedError("TODO(estudiante): deduplicar con row_number por order_id")


deduped_orders = dedupe_orders(clean_orders)
display(deduped_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen
# MAGIC
# MAGIC Cierra el lab con una tabla pequeña, una fila por dimensión, para poder comparar lotes en la próxima ejecución.
# MAGIC
# MAGIC Columnas sugeridas: `dimension`, `metric_name`, `metric_value`, `notes`.
# MAGIC
# MAGIC Métricas mínimas:
# MAGIC
# MAGIC - Completitud: ratio de `order_id` presente en `raw_orders` (no solo `isNotNull`).
# MAGIC - Validez: ratio de filas cuyo `amount` quedó parseado y es `> 0`.
# MAGIC - Consistencia: ratio de filas con `is_consistent`.
# MAGIC - Precisión: ratio de filas con `is_precise`.
# MAGIC - Unicidad: `1 - (filas_validas_extra / filas_validas)` o, equivalentemente, claves distintas / filas en `clean_orders` antes de la ventana.
# MAGIC - Operación: conteo de `quarantine_orders` y conteo final de `deduped_orders`.

# COMMAND ----------

def quality_summary(raw_df, checked_df, clean_df, quarantine_df, deduped_df):
    """Una fila por dimensión, con métricas calculadas (no constantes escritas a mano)."""
    # TODO(estudiante): construye el resumen con spark.createDataFrame o con agg + union.
    # Los números tienen que salir de los DataFrames, no de un literal que hayas contado a ojo.
    raise NotImplementedError("TODO(estudiante): tabla resumen de las cinco dimensiones")


summary = quality_summary(
    raw_orders,
    checked_orders,
    clean_orders,
    quarantine_orders,
    deduped_orders,
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Contrato de salida
# MAGIC
# MAGIC Cuando los TODOs estén hechos, comprueba a mano:
# MAGIC
# MAGIC - `deduped_orders` no tiene dos filas con el mismo `order_id`.
# MAGIC - Para `A-100`, la superviviente es la del `2024-01-16` (importe 80.00), no una de las copias del día 15.
# MAGIC - `A-106` puede quedar en limpio: la diferencia 0.004 está dentro de 0.01.
# MAGIC - `A-101` (ES/USD), `A-104` (estado y cantidad) y la fila sin `order_id` están en cuarentena con el motivo que corresponda.
# MAGIC - Ninguna columna numérica se obtuvo con `.cast` directo sobre el texto crudo.
# MAGIC - No hay secretos en el notebook ni en el historial de git.
# MAGIC
# MAGIC Si una fila admite dos tratamientos razonables (formato europeo, fecha con `/`, alias `USA`), deja el criterio en un comentario de una o dos líneas junto al código.
