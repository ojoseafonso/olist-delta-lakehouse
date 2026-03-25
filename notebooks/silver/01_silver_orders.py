# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------

# Aplicação de schema validado
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False), 
    StructField("order_purchase_timestamp", TimestampType(), False),  
    StructField("order_approved_at", TimestampType(), True), 
    StructField("order_delivered_carrier_date", TimestampType(), True), 
    StructField("order_delivered_customer_date", TimestampType(), True), 
    StructField("order_estimated_delivery_date", TimestampType(), True), 
])

# Lê o arquivo direto do catalog
df_orders = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_orders_dataset.csv")

# COMMAND ----------

# Verifica se nulos existem nas colunas que não podem ter nulos

colunas_para_verificar = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]

# Conta nulos apenas nas colunas especificas
nulos = df_orders.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.orders.{c}: {total_nulos} registros.")


# COMMAND ----------

# Lista de colunas para verificar campos que podem ter nulos
colunas_para_verificar = ["order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]

# Conta nulos apenas nas colunas especificas
nulos = df_orders.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
]).show()
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_orders.count()
distinct_rows = df_orders.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.orders.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_orders.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.orders: {duplicatas} registros duplicados encontrados.")

# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.orders"

df_orders.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
