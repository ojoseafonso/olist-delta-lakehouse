# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, regexp_replace, to_timestamp, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

import re

# Caminho compatível com Python puro no Databricks
filepath = "/Volumes/workspace/olist-storage/bronze/olist_order_reviews_dataset.csv"

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Remove \n dentro de campos entre aspas
content_clean = re.sub(r'"[^"]*"', lambda m: m.group().replace('\n', ' '), content)

# Salva versão sanitizada em path temporário
with open("/Volumes/workspace/olist-storage/bronze/olist_order_reviews_sanitized.csv", "w", encoding="utf-8") as f:
    f.write(content_clean)

print("Arquivo sanitizado salvo com sucesso.")

# COMMAND ----------

# Aplicação de schema
schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", StringType(), False),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), False),
    StructField("review_answer_timestamp", StringType(), False)
])

# Lê o arquivo direto do catalog
df_order_reviews = spark.read.format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .load("/Volumes/workspace/olist-storage/bronze/olist_order_reviews_sanitized.csv") 



# COMMAND ----------

#Supressão das colunas de comentários 

df_order_reviews = df_order_reviews.select(
    "review_id",
    "order_id", 
    "review_score",
    "review_creation_date",
    "review_answer_timestamp"
)

# COMMAND ----------

# Verificação da coluna review_id estar no formato de hash32
df_order_reviews.filter(~(col("review_id").rlike("^[a-f0-9]{32}$"))).show()

# COMMAND ----------

# Pré-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"]

# Conta nulos apenas nas colunas especificas
nulos = df_order_reviews.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in colunas_para_verificar
])

for c in colunas_para_verificar:
    total_nulos = nulos.collect()[0][c]
    if total_nulos > 0:
        raise ValueError(f"[ERRO] Nulos encontrados em silver.reviews.{c}: {total_nulos} registros.")
    

# COMMAND ----------

total = df_order_reviews.count()
validas = df_order_reviews.filter(col("review_id").rlike("^[a-f0-9]{32}$")).count()
print(f"Total: {total} | Válidas: {validas} | Descartadas: {total - validas}")

# COMMAND ----------

# Remove espaços e caracteres não-timestamp antes do cast
for c in ["review_creation_date", "review_answer_timestamp"]:
    df_order_reviews = df_order_reviews \
        .withColumn(c, to_timestamp(trim(col(c)), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# Remover linhas que contêm review_creation_date nulos
df_order_reviews = df_order_reviews.filter(col("review_creation_date").isNotNull())


# COMMAND ----------

# Pós-verificação da lista de colunas para verificar nulos
colunas_para_verificar = ["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"]

# Conta nulos apenas nas colunas especificas
df_order_reviews.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in (colunas_para_verificar)
]).show()
    

# COMMAND ----------

# Verifica linhas duplicadas
total_rows = df_order_reviews.count()
distinct_rows = df_order_reviews.distinct().count()
duplicate_count = total_rows - distinct_rows
if duplicate_count > 0:
    raise ValueError(f"[ERRO] {duplicate_count} linhas duplicadas encontradas em silver.reviews.")

# COMMAND ----------

# Verifica PK duplicada
df_duplicados = df_order_reviews.groupBy("review_id", "order_id") \
    .count() \
    .filter(col("count") > 1)

duplicatas = df_duplicados.count()

if duplicatas > 0:
    raise ValueError(f"[ERRO] PK duplicada em silver.reviews: {duplicatas} registros duplicados encontrados.")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.silver.order_reviews"
df_order_reviews.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("olist.silver.order_reviews")