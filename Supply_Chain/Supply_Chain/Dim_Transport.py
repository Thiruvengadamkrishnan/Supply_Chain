# Databricks notebook source
df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df_dim_transport = (
    df_silver
    .select("transport_mode")
    .distinct()
    .withColumn("transport_key", monotonically_increasing_id())
)

df_dim_transport.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.dim_transport")

# COMMAND ----------

display(df_dim_transport)