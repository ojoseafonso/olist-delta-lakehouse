# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest, lag, lead, expr
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# Lê o arquivo direto do catalog
dim_clientes = spark.read.table("olist.silver.customers")
dim_orders = spark.read.table("olist.silver.orders")

# COMMAND ----------

#Join entre customer e orders
left_df = (
    dim_clientes.join(dim_orders, on=['customer_id'], how='left')
    .orderBy('order_purchase_timestamp'))

# COMMAND ----------

# Aplicando LAG na coluna order_purchase_timestamp para DATA_INICIO

window = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")

left_df = left_df.withColumn("zip_anterior", lag("customer_zip_code_prefix").over(window))

# Identifica mudança de endereço
left_df = left_df.withColumn(
    "mudou_endereco",
    when(col("zip_anterior").isNull(), True) \
    .when(col("customer_zip_code_prefix") != col("zip_anterior"),True)
    .otherwise(False)
) \
.withColumn("data_inicio", when(col("mudou_endereco") == True, col("order_purchase_timestamp"))) \
.filter(col("mudou_endereco") == True)
#.withColumn("versão", when(col("mudou_endereco") == True | col("zip_anterior").isNull(), col("order_purchase_timestamp"))) \


# COMMAND ----------

# Aplicando LEAD na coluna order_purchase_timestamp para DATA_FIM

window_versao = Window.partitionBy("customer_unique_id").orderBy("data_inicio")

left_df = left_df.withColumn(
    "data_fim",
    (lead("data_inicio").over(window_versao) - expr("INTERVAL 1 DAY"))
)


# COMMAND ----------

# Identifica versão da mudança de endereço, data_inicio
dim_cliente = left_df.withColumn(
    "is_current",
    when(col("data_fim").isNull(), True).otherwise(False)
)

# COMMAND ----------

# Criação de SK Hash32
dim_cliente = dim_cliente.withColumn("sk_cliente",sha2(concat(lit("dim_cliente||"), col("customer_zip_code_prefix"), col("customer_unique_id"), col("data_inicio").cast("string")), 256))

# COMMAND ----------

dim_cliente = dim_cliente.select(
    "sk_cliente",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "data_inicio",
    "data_fim",
    "is_current"
)

# COMMAND ----------

# Salva o df como delta
table_name = "olist.gold.dim_cliente"
dim_cliente.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("olist.gold.dim_cliente")