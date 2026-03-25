# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest, lag, lead, expr
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# Lê o arquivo direto do catalog

dim_cliente = spark.read.table("olist.gold.dim_cliente")
dim_tempo = spark.read.table("olist.gold.dim_tempo")

payments = spark.read.table("olist.silver.order_payments")
orders = spark.read.table("olist.silver.orders")
customers = spark.read.table("olist.silver.customers")



# COMMAND ----------

#Join entre pagamentos e pedidos (SILVER)
left_df = (
    payments.join(orders, on=['order_id'], how='left'))

# COMMAND ----------

#Join entre anterior e customers (SILVER)
left_df = (
    left_df.join(customers, on=['customer_id'], how='left')) \
   ## .withColumnRenamed("customer_zip_code_prefix", "customer_zip_code_prefix_silver")


#Join anterior com SCD Tipo 2 cliente
left_df = (
    left_df.join(dim_cliente, on=((left_df['customer_unique_id'] == dim_cliente['customer_unique_id']) \
                                & (left_df['order_purchase_timestamp'] >= dim_cliente['data_inicio']) \
                                & ((left_df['order_purchase_timestamp'] < dim_cliente['data_fim']) | (dim_cliente['data_fim'].isNull()))),how='left')) \
                                .drop("customer_id","customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state", "data_inicio", "data_fim", "is_current")


# COMMAND ----------

#Join entre anterior e tempo
left_df = (left_df.join(dim_tempo.select("calendar_date", "sk_tempo"), on=(left_df['order_purchase_timestamp'].cast("date") == dim_tempo['calendar_date']), how='left')).withColumnRenamed("sk_tempo", "sk_tempo_compra").withColumn("sk_tempo_compra", 
    coalesce(col("sk_tempo_compra"), lit("0"))).drop("order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date","calendar_date")


# COMMAND ----------

fato_pagamentos = left_df.select("order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value", "sk_cliente", "sk_tempo_compra")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.gold.fato_pagamentos"
fato_pagamentos.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)