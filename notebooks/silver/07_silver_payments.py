# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", StringType(), False),
    StructField("payment_value", StringType(), False)
])

# Lê o arquivo direto do catalog
df_order_payments = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_order_payments_dataset.csv")

# COMMAND ----------

# Pré-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]

# Conta nulos apenas nas colunas especificas
df_order_payments.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
]).show()
    

# COMMAND ----------

# Limpa caracteres de texto nas colunas numéricas e substitui , por . e converte para DoubleType

for c in ["payment_value"]:
    df_order_payments = df_order_payments \
        .withColumn(c, regexp_replace(col(c), "[^0-9.,]", "")) \
        .withColumn(c, regexp_replace(col(c), ",", ".")) \
        .withColumn(c, col(c).cast(DoubleType()))

# COMMAND ----------

# Covnersão de string para int nas colunas de parcelas
df_order_payments = df_order_payments \
    .withColumn("payment_sequential", col("payment_sequential").cast(IntegerType())) \
    .withColumn("payment_installments", col("payment_installments").cast(IntegerType()))

# COMMAND ----------

# Pós-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]

# Conta nulos apenas nas colunas especificas
nulos = df_order_payments.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.payments.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_order_payments.count()
distinct_rows = df_order_payments.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.payments.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_order_payments.groupBy("order_id", "payment_sequential") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.payments: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.order_payments"
df_order_payments.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
