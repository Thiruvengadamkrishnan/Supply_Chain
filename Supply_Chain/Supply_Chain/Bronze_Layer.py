# Databricks notebook source
# MAGIC %md
# MAGIC # **Read the Unstructured File Properly**

# COMMAND ----------

df_src = spark.read.format("text").load(
    "/Volumes/supply_chain/bronze/dataset/Supply_Chain/car_supply_unstructured_1000.txt"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Display The Source File**

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Remove Empty Rows**

# COMMAND ----------

from pyspark.sql.functions import col

df_src = df_src.filter(col("value").isNotNull() & (col("value") != ""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Bronze Metadata Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_bronze = df_src \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("file_name", col("_metadata.file_name")) \
    .withColumn("ingestion_ts", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Bronze Delta Table

# COMMAND ----------

df_src.select("_metadata.*").display()

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.car_supply_raw")

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # To Know the Columns

# COMMAND ----------

df_bronze.printSchema()