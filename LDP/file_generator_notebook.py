# Databricks notebook source
# MAGIC %md
# MAGIC ## File Generator for DLT/Lakeflow Demo
# MAGIC Target Volume: `/gambill_data_demo/core/drops/file_ingestion`

# COMMAND ----------

from pyspark.sql import SparkSession
import random, string
from datetime import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze_dev.dlt_demo_files;
# MAGIC create volume if not exists bronze_dev.dlt_demo_files.demo_synthetic_data;

# COMMAND ----------

catalog = "bronze_dev"
schema = "dlt_demo_files"
volume = "demo_synthetic_data"
subdir = "file_ingestion"
VOLUME_PATH = f"/Volumes/{catalog}/{schema}/{volume}/{subdir}"

# COMMAND ----------

def rand_email():
    import random, string
    user = ''.join(random.choices(string.ascii_lowercase, k=8))
    domain = random.choice(["example.com","corp.local","mail.test"])
    return f"{user}@{domain}"

def make_records(n=250):
    ts = datetime.utcnow().isoformat() + "Z"
    import random
    names = ["Ava","Ben","Chris","Dee","Eli","Finn","Gia","Hari","Ivy","Jae"]
    return [{"id": i, "name": random.choice(names), "email": rand_email() if random.random() > 0.05 else None, "timestamp": ts} for i in range(n)]

# COMMAND ----------

df = spark.createDataFrame(make_records())

try:
    dbutils.fs.mkdirs(VOLUME_PATH)
except Exception:
    pass

# COMMAND ----------

out_json = f"{VOLUME_PATH}/batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
df.coalesce(1).write.mode("append").json(out_json)

# COMMAND ----------

out_csv = f"{VOLUME_PATH}/batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
df.coalesce(1).write.mode("append").option("header", True).csv(out_csv)

# COMMAND ----------

print("Wrote files to:", VOLUME_PATH)
