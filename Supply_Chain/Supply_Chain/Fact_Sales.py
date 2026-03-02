# Databricks notebook source
df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

from pyspark.sql.functions import col, date_format

df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

df_dim_location = spark.read.table("supply_chain.silver.dim_location")
df_dim_vehicle = spark.read.table("supply_chain.silver.dim_vehicle")
df_dim_transport = spark.read.table("supply_chain.silver.dim_transport")

# COMMAND ----------

from pyspark.sql.functions import col, date_format

df_fact = (
    df_silver
    
    # Join Dimensions
    .join(df_dim_location, ["city", "country"], "left")
    .join(df_dim_vehicle, ["vehicle_brand", "vehicle_model", "vehicle_color"], "left")
    .join(df_dim_transport, ["transport_mode"], "left")
    
    # Proper Date Keys
    .withColumn(
        "order_date_key",
        date_format(col("order_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "shipment_date_key",
        date_format(col("shipment_date"), "yyyyMMdd").cast("int")
    )
    
    # Select Fact Columns
    .select(
        "order_key",
        "order_date_key",
        "shipment_date_key",
        "location_key",
        "vehicle_key",
        "transport_key",
        "delay_days",
        "rating",
        "delivery_duration_days",
        "is_late_delivery",
        "inventory_verified",
        "payment_confirmed"
    )
)

df_fact.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.fact_orders")

# COMMAND ----------

df_fact.display()