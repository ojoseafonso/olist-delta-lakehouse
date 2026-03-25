# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False)
])

# Lê o arquivo direto do catalog
df_product_category_name_translation = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/product_category_name_translation.csv")

# COMMAND ----------

# Verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["product_category_name", "product_category_name_english"]

# Conta nulos apenas nas colunas especificas
nulos = df_product_category_name_translation.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.product_category_name_translation.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_product_category_name_translation.count()
distinct_rows = df_product_category_name_translation.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.product_category_name_translation.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_product_category_name_translation.groupBy("product_category_name") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.product_category_name_translation: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.product_category_name_translation"
df_product_category_name_translation.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
