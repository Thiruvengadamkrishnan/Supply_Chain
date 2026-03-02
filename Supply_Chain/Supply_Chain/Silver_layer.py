# Databricks notebook source
from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    regexp_replace,
    split,
    explode,
    trim,
    regexp_extract,
    to_date
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze

# COMMAND ----------

df_bronze = spark.read.table("supply_chain.bronze.car_supply_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine all rows into single text

# COMMAND ----------

df_combined = df_bronze.agg(
    concat_ws(" ", collect_list("value")).alias("combined_value")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Normalize text (remove extra spaces / line breaks)

# COMMAND ----------

df_normalized = df_combined.withColumn(
    "combined_value",
    regexp_replace(col("combined_value"), r"\s+", " ")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Split records using 'final release.'
# MAGIC

# COMMAND ----------


df_split = df_normalized.select(
    explode(
        split(
            col("combined_value"),
            r"(?<=final release\.)\s+"
        )
    ).alias("record")
)

df_split = df_split.filter(trim(col("record")) != "")

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove leading numbering (1. 2. 3. ...)

# COMMAND ----------

df_clean = df_split.withColumn(
    "record",
    regexp_replace(col("record"), r"^\s*\d+\.\s*", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Extract Structured Columns

# COMMAND ----------

df_extracted = (
    df_clean
    
    # Order Date
    .withColumn(
        "order_date_str",
        regexp_extract(col("record"), r"On ([A-Za-z]+ \d{1,2}, \d{4})", 1)
    )
    .withColumn(
        "order_date",
        to_date(col("order_date_str"), "MMMM d, yyyy")
    )
    
    # City & Country
    .withColumn(
        "city",
        regexp_extract(col("record"), r"customer from ([A-Za-z\s]+),", 1)
    )
    .withColumn(
        "country",
        regexp_extract(col("record"), r"customer from [A-Za-z\s]+, ([A-Za-z\s]+) placed", 1)
    )
    
    # Vehicle Info
    .withColumn(
        "vehicle_color",
        regexp_extract(col("record"), r"order for a ([A-Za-z]+)", 1)
    )
    .withColumn(
        "vehicle_brand",
        regexp_extract(col("record"), r"order for a [A-Za-z]+ ([A-Za-z]+)", 1)
    )
    .withColumn(
        "vehicle_model",
        regexp_extract(col("record"), r"order for a [A-Za-z]+ [A-Za-z]+ ([A-Za-z0-9]+)", 1)
    )
    
    # Transport Mode
    .withColumn(
        "transport_mode",
        regexp_extract(col("record"), r"dispatch via ([A-Za-z\s]+)\.", 1)
    )
    
    # Shipment Date
    .withColumn(
        "shipment_date_str",
        regexp_extract(col("record"), r"departed on ([A-Za-z]+ \d{1,2}, \d{4})", 1)
    )
    .withColumn(
        "shipment_date",
        to_date(col("shipment_date_str"), "MMMM d, yyyy")
    )
    
    # Delay Days
    .withColumn(
        "delay_days",
        regexp_extract(col("record"), r"delay of (\d+) days", 1).cast("int")
    )
    
    # Rating
    .withColumn(
        "rating",
        regexp_extract(col("record"), r"rating of (\d+) out of", 1).cast("int")
    )
    .withColumn(
        "max_rating",
        regexp_extract(col("record"), r"out of (\d+)", 1).cast("int")
    )
    
    # Flags
    .withColumn(
        "inventory_verified",
        col("record").contains("inventory verification")
    )
    .withColumn(
        "payment_confirmed",
        col("record").contains("payment confirmation")
    )
    
    .drop("order_date_str", "shipment_date_str")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Final Output

# COMMAND ----------

df_extracted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # To Show to Columns and Datatypes

# COMMAND ----------

df_extracted.printSchema()

# COMMAND ----------

from pyspark.sql.functions import (
    col, upper, initcap, datediff, current_timestamp, monotonically_increasing_id
)

df_silver_clean = (
    df_extracted
    
    # Remove null critical fields
    .filter(col("order_date").isNotNull())
    .filter(col("shipment_date").isNotNull())
    .filter(col("city") != "")
    
    # Standardize case
    .withColumn("city", initcap(col("city")))
    .withColumn("country", upper(col("country")))
    .withColumn("vehicle_brand", initcap(col("vehicle_brand")))
    .withColumn("vehicle_color", initcap(col("vehicle_color")))
    .withColumn("transport_mode", initcap(col("transport_mode")))
    
    # Delivery Duration
    .withColumn(
        "delivery_duration_days",
        datediff(col("shipment_date"), col("order_date"))
    )
    
    # Late Delivery Flag
    .withColumn(
        "is_late_delivery",
        (col("delay_days") > 0)
    )
    
    # Surrogate Key
    .withColumn(
        "order_key",
        monotonically_increasing_id()
    )
    
    # Audit Columns
    .withColumn("record_created_at", current_timestamp())
)

df_silver_clean.display()

# COMMAND ----------

df_silver_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.car_orders_clean")

# COMMAND ----------

df_silver_clean.printSchema()