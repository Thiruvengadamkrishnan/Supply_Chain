# Databricks notebook source
df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df_dim_vehicle = (
    df_silver
    .select("vehicle_brand", "vehicle_model", "vehicle_color")
    .distinct()
    .withColumn("vehicle_key", monotonically_increasing_id())
)

df_dim_vehicle.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.dim_vehicle")

# COMMAND ----------

display(df_dim_vehicle)

# COMMAND ----------

