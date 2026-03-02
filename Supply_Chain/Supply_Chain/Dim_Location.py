# Databricks notebook source
df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_dim_location = (
    df_silver
    .select("city", "country")
    .distinct()
    .withColumn("location_key", monotonically_increasing_id())
)

df_dim_location.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.dim_location")

# COMMAND ----------

df_dim_location.display()