# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

# Aplicação de schema validado
schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), False),
    StructField("product_name_lenght", StringType(), False), 
    StructField("product_description_lenght", StringType(), False), 
    StructField("product_photos_qty", StringType(), False), 
    StructField("product_weight_g", StringType(), False),
    StructField("product_length_cm", StringType(), False),
    StructField("product_height_cm", StringType(), False),
    StructField("product_width_cm", StringType(), False),
])

# Lê o arquivo direto do catalog
df_products = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_products_dataset.csv")

# COMMAND ----------

# Pré-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]

# Conta nulos apenas nas colunas especificas
df_products.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
]).show()
    

# COMMAND ----------

# Preenche categoria nula com valor sentinela
df_products = df_products.withColumn(
    "product_category_name",
    when(col("product_category_name").isNull(), "sem_categoria")
    .otherwise(col("product_category_name"))
)

# COMMAND ----------

# Limpa caracteres de texto nas colunas numéricas e substitui , por . e converte para DoubleType

for c in ["product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
    df_products = df_products \
        .withColumn(c, regexp_replace(col(c), "[^0-9.,]", "")) \
        .withColumn(c, regexp_replace(col(c), ",", ".")) \
        .withColumn(c, col(c).cast(DoubleType()))

# COMMAND ----------

# Pós-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["product_id", "product_category_name"]

# Conta nulos apenas nas colunas especificas
nulos = df_products.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.products.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_products.count()
distinct_rows = df_products.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.products.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_products.groupBy("product_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.products: {duplicatas} registros duplicados encontrados.")

# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.products"
df_products.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
