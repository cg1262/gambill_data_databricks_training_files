# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT Python: File Drop → Bronze → Silver → Gold
# MAGIC Target: `bronze_dev.dlt_demo_files` | Storage: `/Volumes/bronze_dev/dlt_demo_files/dlt_storage`
# MAGIC
# MAGIC This notebook shows the same pipeline you have in SQL/YAML, but defined with the **DLT Python API**.
# MAGIC It reads JSON and CSV from a **Volume** using Auto Loader (`cloudFiles`), applies expectations in Silver,
# MAGIC and produces a Gold aggregate.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/bronze_dev/dlt_demo_files/demo_synthetic_data/file_ingestion"

cloud_files_opts_json = {
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.includeExistingFiles": "true",
  "cloudFiles.schemaEvolutionMode": "addNewColumns"
}
cloud_files_opts_csv = {
  **cloud_files_opts_json,
  "header": "true"
}

# COMMAND ----------
@dlt.table(
  name="bronze_raw",
  comment="Raw union of JSON and CSV drops from a Volume.",
  table_properties={"quality":"bronze"}
)
def bronze_raw():
    # Use glob filters so JSON reader won't grab CSV files and vice versa
    json_df = (spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .options(**cloud_files_opts_json)
               .load(f"{VOLUME_PATH}/*.json"))

    csv_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .options(**cloud_files_opts_csv)
              .load(f"{VOLUME_PATH}/*.csv"))

    return json_df.select("*").unionByName(csv_df.select("*"), allowMissingColumns=True)

# COMMAND ----------
@dlt.table(
  name="silver_clean",
  comment="Cleaned records with normalized email and typed timestamps.",
  table_properties={"quality":"silver"}
)
@dlt.expect_or_drop("email_not_null", "email IS NOT NULL")
@dlt.expect("id_must_exist", "id IS NOT NULL")
def silver_clean():
    return (dlt.read_stream("bronze_raw")
            .withColumn("email_normalized", F.lower(F.trim(F.col("email"))))
            .withColumn("event_ts", F.to_timestamp("timestamp"))
            .withColumn("load_date", F.to_date(F.col("event_ts")))
            # Watermark required for streaming aggregations
            .withWatermark("event_ts", "1 day")
           )

# COMMAND ----------
@dlt.table(
  name="gold_daily_users",
  comment="Daily approximate unique users from Silver (streaming-safe).",
  table_properties={"quality":"gold"}
)
def gold_daily_users():
    return (dlt.read_stream("silver_clean")
            .groupBy(F.window("event_ts", "1 day").alias("w"))
            .agg(F.approx_count_distinct("email_normalized").alias("unique_users"))
            .select(F.to_date(F.col("w.start")).alias("load_date"), F.col("unique_users"))
           )


