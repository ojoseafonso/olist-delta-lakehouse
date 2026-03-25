# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", StringType(), False),
    StructField("price", StringType(), False),
    StructField("freight_value", StringType(), False)   
])

# Lê o arquivo direto do catalog
df_order_items = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_order_items_dataset.csv")

# COMMAND ----------

# Pré-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]

# Conta nulos apenas nas colunas especificas
df_order_items.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
]).show()
    

# COMMAND ----------

# Limpa caracteres de texto nas colunas numéricas e substitui , por . e converte para DoubleType

for c in ["price", "freight_value"]:
    df_order_items = df_order_items \
        .withColumn(c, regexp_replace(col(c), "[^0-9.,]", "")) \
        .withColumn(c, regexp_replace(col(c), ",", ".")) \
        .withColumn(c, col(c).cast(DoubleType()))

# COMMAND ----------

# Pós-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]

# Conta nulos apenas nas colunas especificas
nulos = df_order_items.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.order_items.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_order_items.count()
distinct_rows = df_order_items.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.order_items.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_order_items.groupBy("order_item_id", "order_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.order_items: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.order_items"
df_order_items.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
