# Databricks notebook source
# MAGIC %md
# MAGIC # Create a Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists supply_chain

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists supply_chain.bronze;
# MAGIC create schema if not exists  supply_chain.silver;
# MAGIC create schema if not exists supply_chain.gold;