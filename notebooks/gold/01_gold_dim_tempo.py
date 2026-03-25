# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row

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
df_orders = spark.read.table("olist.silver.orders")

# COMMAND ----------

# Pega o mínimo por linha entre as 4 colunas, ignorando nulos
df_min = df_orders.withColumn(
    "min_date",
    least(
        coalesce(col("order_purchase_timestamp"), lit("9999-12-31").cast(TimestampType())),
        coalesce(col("order_approved_at"), lit("9999-12-31").cast(TimestampType())),
        coalesce(col("order_delivered_customer_date"), lit("9999-12-31").cast(TimestampType())),
        coalesce(col("order_estimated_delivery_date"), lit("9999-12-31").cast(TimestampType()))
    )
)

# Pega o mínimo global
min_date = df_min.agg(spark_min("min_date")).collect()[0][0]


# Min já coletados como objetos Python datetime
min_date = df_min.agg(spark_min("min_date")).collect()[0][0]


# Deriva o range: primeiro dia do ano mínimo até 31/12 do ano máximo + 10
start_date = f"{min_date.year}-01-01"
end_date = f"{min_date.year + 15}-12-31"

print(f"Range: {start_date} até {end_date}")

# Gera o date spine via Spark SQL
dates_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        INTERVAL 1 DAY
    )) AS calendar_date
""")


# COMMAND ----------


# Mapeando nomes dos meses em português-br
meses = create_map([
    lit(1), lit("janeiro"), lit(2), lit("fevereiro"),
    lit(3), lit("março"), lit(4), lit("abril"),
    lit(5), lit("maio"), lit(6), lit("junho"),
    lit(7), lit("julho"), lit(8), lit("agosto"),
    lit(9), lit("setembro"), lit(10), lit("outubro"),
    lit(11), lit("novembro"), lit(12), lit("dezembro")
])

# Mapeando nomes dos dias da semana em português-br
dias = create_map([
    lit(1), lit("domingo"), lit(2), lit("segunda-feira"),
    lit(3), lit("terça-feira"), lit(4), lit("quarta-feira"),
    lit(5), lit("quinta-feira"), lit(6), lit("sexta-feira"),
    lit(7), lit("sábado")
])

# Criando as derivações de datas
dates_df = dates_df.withColumn("ano", year(col("calendar_date"))) \
                   .withColumn("trimestre", quarter(col("calendar_date"))) \
                   .withColumn("mes", month(col("calendar_date"))) \
                   .withColumn("nome_mes", meses[col("mes")])  \
                   .withColumn("semana_do_ano", weekofyear(col("calendar_date"))) \
                   .withColumn("dia", dayofmonth(col("calendar_date"))) \
                   .withColumn("dia_da_semana", dayofweek(col("calendar_date"))) \
                   .withColumn("nome_dia", dias[col("dia_da_semana")])
               

# COMMAND ----------

# Criação de SK Hash32
dates_df = dates_df.withColumn("sk_tempo",sha2(concat(lit("dim_tempo||"), col("calendar_date").cast("string")), 256))

# COMMAND ----------

# Definindo schema sentinela
schema_sentinela = StructType([StructField("sk_tempo", StringType(), True),
                               StructField("calendar_date", DateType(), True),
                               StructField("ano", IntegerType(), True),
                               StructField("trimestre", IntegerType(), True),
                               StructField("mes", IntegerType(), True),
                               StructField("nome_mes", StringType(), True),
                               StructField("semana_do_ano", IntegerType(), True),
                               StructField("dia", IntegerType(), True),
                               StructField("dia_da_semana", IntegerType(), True),
                               StructField("nome_dia", StringType(), True)])

# Linha sentinela
sentinela = spark.createDataFrame([Row(
    sk_tempo="0",
    calendar_date=None,
    ano=None,
    trimestre=None,
    mes=None,
    nome_mes="data desconhecida",
    semana_do_ano=None,
    dia=None,
    dia_da_semana=None,
    nome_dia=None
)], schema=schema_sentinela)

dim_tempo = dates_df.unionByName(sentinela)


# COMMAND ----------

# Salva o df como delta
table_name = "olist.gold.dim_tempo"
dim_tempo.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)