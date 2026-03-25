# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace, lower, avg, translate, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.window import Window

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), False),
    StructField("geolocation_lat", StringType(), False),
    StructField("geolocation_lng", StringType(), False),
    StructField("geolocation_city", StringType(), False),
    StructField("geolocation_state", StringType(), False)
])

# Lê o arquivo direto do catalog
df_geolocation = spark.read.format("csv").schema(schema).option("header", "true").load("/Volumes/workspace/olist-storage/bronze/olist_geolocation_dataset.csv")

# COMMAND ----------

# Pré-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]

# Conta nulos apenas nas colunas especificas
df_geolocation.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
]).show()
    

# COMMAND ----------

# Limpa caracteres de texto nas colunas numéricas e converte para DoubleType

for c in ["geolocation_city", "geolocation_state"]:
    df_geolocation = df_geolocation \
        .withColumn(c, lower(col(c)))

# COMMAND ----------

# Padroniza o texto das colunas cidade e estado

for c in ["geolocation_city", "geolocation_state"]:

    # Define mappings
    accented = 'ãäöüßáäčďéěíĺľňóôŕšťúůýžÄÖÜßÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ'
    normal =   'aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ'

    df_geolocation = df_geolocation \
        .withColumn(c, translate(col(c), accented, normal)) \
        .withColumn(c, lower(col(c),))


# COMMAND ----------

# Implementa o filtro de apenas uma cidade, a maior em caso de colunas iguais 

df_geolocation = df_geolocation.groupBy("geolocation_zip_code_prefix").agg(
    spark_max("geolocation_city").alias("geolocation_city"),
    spark_max("geolocation_state").alias("geolocation_state"),
    avg("geolocation_lat").alias("mean_latitude"),
    avg("geolocation_lng").alias("mean_longitude"))

# COMMAND ----------

#Supressão das colunas originais de lat e long

df_geolocation =df_geolocation.drop(
    "geolocation_lat", "geolocation_lng"
)

# COMMAND ----------

# Pós-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["geolocation_zip_code_prefix", "mean_latitude", "mean_longitude", "geolocation_city", "geolocation_state"]

# Conta nulos apenas nas colunas especificas
nulos = df_geolocation.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.geolocation.{c}: {total_nulos} registros.")
    

# COMMAND ----------

# Remove as linhas duplicadas
df_geolocation = df_geolocation.dropDuplicates()

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_geolocation.count()
distinct_rows = df_geolocation.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.geolocation.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_geolocation.groupBy("geolocation_zip_code_prefix","mean_longitude","mean_latitude") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.geolocation: {duplicatas} registros duplicados encontrados.")

# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.geolocation"
df_geolocation.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
