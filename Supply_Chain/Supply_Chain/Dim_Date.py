# Databricks notebook source
df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

from pyspark.sql.functions import col, year, month, quarter, dayofmonth, date_format

df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

df_dim_date = (
    df_silver
    .select(col("order_date").alias("date"))
    .union(
        df_silver.select(col("shipment_date").alias("date"))
    )
    .distinct()
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("day", dayofmonth("date"))
)

df_dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.dim_date")

# COMMAND ----------

df_dim_date.display()