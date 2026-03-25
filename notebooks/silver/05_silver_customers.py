# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), False),
    StructField("customer_city", StringType(), False),
    StructField("customer_state", StringType(), False)
])

# Lê o arquivo direto do catalog
df_customers = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_customers_dataset.csv")

# COMMAND ----------

# Verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]


# Conta nulos apenas nas colunas especificas
nulos = df_customers.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.customers.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_customers.count()
distinct_rows = df_customers.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.customers.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_customers.groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.customers: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.customers"
df_customers.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
