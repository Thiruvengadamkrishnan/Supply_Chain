# Databricks notebook source
# MAGIC %md
# MAGIC gold_order_kpis

# COMMAND ----------

df_silver = spark.read.table("supply_chain.silver.car_orders_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supply_chain.silver.car_orders_clean;

# COMMAND ----------

from pyspark.sql.functions import avg, count, sum,col

df_gold_kpi = (
    df_silver
    .groupBy()
    .agg(
        count("*").alias("total_orders"),
        (avg(col("is_late_delivery").cast("int")) * 100).alias("late_delivery_pct"),
        avg("delay_days").alias("avg_delay_days"),
        avg("rating").alias("avg_customer_rating")
    )
)

df_gold_kpi.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.order_summary_kpis")

# COMMAND ----------

display(df_gold_kpi)

# COMMAND ----------

# MAGIC %md
# MAGIC Brand Performance

# COMMAND ----------

df_gold_brand = (
    df_silver
    .groupBy("vehicle_brand")
    .agg(
        count("*").alias("total_orders"),
        avg("rating").alias("avg_rating"),
        avg("delay_days").alias("avg_delay")
    )
)

df_gold_brand.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.brand_performance")

# COMMAND ----------

display(df_gold_brand)