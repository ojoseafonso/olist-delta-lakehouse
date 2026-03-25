# Databricks notebook source
from pyspark.sql.functions import col, sum, isnan, when, count, least, min as spark_min, coalesce, lit, trunc, date_format, make_date, max as spark_max, year, quarter, month, weekofyear, dayofmonth, dayofweek, create_map, sha2, concat_ws, concat, greatest, lag, lead, expr
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# Lê o arquivo direto do catalog
dim_pedidos = spark.read.table("olist.silver.orders")
dim_itens_pedidos = spark.read.table("olist.silver.order_items")
dim_seller = spark.read.table("olist.gold.dim_seller")
dim_produto = spark.read.table("olist.gold.dim_produto")
dim_cliente = spark.read.table("olist.gold.dim_cliente")
dim_tempo = spark.read.table("olist.gold.dim_tempo")

orders = spark.read.table("olist.silver.orders")
order_items = spark.read.table("olist.silver.order_items")
customers = spark.read.table("olist.silver.customers")



# COMMAND ----------

#Join entre pedidos e itens de pedido (SILVER)
left_df = (
    order_items.join(orders, on=['order_id'], how='left'))

# COMMAND ----------

#Join entre anterior e customers (SILVER)
left_df = (
    left_df.join(customers, on=['customer_id'], how='left')) \
   ## .withColumnRenamed("customer_zip_code_prefix", "customer_zip_code_prefix_silver")


# COMMAND ----------

#Join anterior com SCD Tipo 2 
left_df = (
    left_df.join(dim_cliente, on=((left_df['customer_unique_id'] == dim_cliente['customer_unique_id']) \
                                & (left_df['order_purchase_timestamp'] >= dim_cliente['data_inicio']) \
                                & ((left_df['order_purchase_timestamp'] < dim_cliente['data_fim']) | (dim_cliente['data_fim'].isNull()))),how='left')) \
                                .drop("customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state", "data_inicio", "data_fim", "is_current")



# COMMAND ----------

#Join anterior e produto
left_df = (
    left_df.join(dim_produto, on=['product_id'], how='left')) \
           .drop("product_category_name", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm")


# COMMAND ----------

#Join anterior e seller
left_df = (left_df.join(dim_seller, on=['seller_id'], how='left')) \
                  .drop("seller_zip_code_prefix", "seller_city", "seller_state")    


# COMMAND ----------

#Join anterior e data sk 
left_df_data1 = (left_df.join(dim_tempo.select("calendar_date", "sk_tempo"), on=(left_df['order_purchase_timestamp'].cast("date") == dim_tempo['calendar_date']), how='left')).withColumnRenamed("sk_tempo", "sk_tempo_compra").drop("calendar_date")

left_df_data2 = (left_df_data1.join(dim_tempo.select("calendar_date", "sk_tempo"), on=(left_df_data1['order_approved_at'].cast("date") == dim_tempo['calendar_date']), how='left')).withColumnRenamed("sk_tempo", "sk_tempo_aprovacao").withColumn("sk_tempo_aprovacao", 
    coalesce(col("sk_tempo_aprovacao"), lit("0"))).drop("calendar_date")

left_df_data3 = (left_df_data2.join(dim_tempo.select("calendar_date", "sk_tempo"), on=(left_df_data2['order_delivered_customer_date'].cast("date") == dim_tempo['calendar_date']), how='left')).withColumnRenamed("sk_tempo", "sk_tempo_entrega").withColumn("sk_tempo_entrega", 
    coalesce(col("sk_tempo_entrega"), lit("0"))).drop("calendar_date")

left_df_data4 = (left_df_data3.join(dim_tempo.select("calendar_date", "sk_tempo"), on=(left_df_data3['order_estimated_delivery_date'].cast("date") == dim_tempo['calendar_date']), how='left')).withColumnRenamed("sk_tempo", "sk_tempo_entrega_estimada").withColumn("sk_tempo_entrega_estimada", 
    coalesce(col("sk_tempo_entrega_estimada"), lit("0"))).drop("calendar_date")


# COMMAND ----------

fato_itens_pedido = left_df_data4.select("sk_produto", "sk_seller", "sk_cliente", "sk_tempo_compra", "sk_tempo_aprovacao", "sk_tempo_entrega_estimada", "sk_tempo_entrega", "order_id", "order_item_id", "order_status", "price", "freight_value")


# COMMAND ----------

# Salva o df como delta
table_name = "olist.gold.fato_itens_pedido"
fato_itens_pedido.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)