# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest, lag, lead, expr
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# Lê o arquivo direto do catalog
dim_produto = spark.read.table("olist.silver.products")

# COMMAND ----------

# Deduplicação pela chave natural
dim_produto = dim_produto.dropDuplicates(["product_id"])

# COMMAND ----------

# Criação de SK Hash32
dim_produto = dim_produto.withColumn("sk_produto",sha2(concat(lit("dim_produto||"), col("product_id").cast("string")), 256))

# COMMAND ----------

dim_produto = dim_produto.select(
    "sk_produto",
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm")

# COMMAND ----------

# Salva o df como delta
table_name = "olist.gold.dim_produto"
dim_produto.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)