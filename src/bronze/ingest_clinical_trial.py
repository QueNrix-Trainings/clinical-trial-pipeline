# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: clinical trial ingestion (landing parquet to Delta)

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "clinical_dev")
dbutils.widgets.text("landing_path", "abfss://raw@landingstorageac.dfs.core.windows.net/clinical_trial/")

catalog = dbutils.widgets.get("catalog")
landing = dbutils.widgets.get("landing_path")
ops = f"/Volumes/{catalog}/bronze/ops/clinical_trial_raw"

# COMMAND ----------
df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", f"{ops}/schema")
      .load(landing)
      .withColumn("_ingest_time", F.current_timestamp())
      .withColumn("_source_file", F.col("_metadata.file_path")))

(df.writeStream
   .option("checkpointLocation", f"{ops}/checkpoint")
   .option("mergeSchema", "true")
   .trigger(availableNow=True)
   .toTable(f"{catalog}.bronze.clinical_trial_raw"))