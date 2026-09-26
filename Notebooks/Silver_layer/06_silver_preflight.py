# Databricks notebook source
# MAGIC %md
# MAGIC # Silver preflight
# MAGIC Job task 1. Read-only audit of the current Bronze snapshot. Any conflicting
# MAGIC duplicate or wrong Bronze schema stops the Job before the pipeline task.

# COMMAND ----------
import json
import sys

for name, default in {
    "source_path": "", "bronze_table": "github_lakehouse.bronze.github_events_raw",
    "source_start_date": "", "source_end_date": "", "source_file_name": "",
    "expected_source_files": "0",
}.items():
    dbutils.widgets.text(name, default)
params = {name: dbutils.widgets.get(name) for name in [
    "source_path", "bronze_table", "source_start_date", "source_end_date",
    "source_file_name", "expected_source_files",
]}
if params["source_path"]:
    sys.path.insert(0, params["source_path"])
from pyspark.sql import functions as F
from silver_contract import identifier, require_bronze_schema, scoped_source
from silver_validation import check_duplicate_content

spark.conf.set("spark.sql.session.timeZone", "UTC")
bronze_table = identifier(params["bronze_table"], 3)
version = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").first()["version"]
source = spark.read.option("versionAsOf", version).table(bronze_table)
require_bronze_schema(source)
source = scoped_source(source, params["source_start_date"], params["source_end_date"], params["source_file_name"])

# COMMAND ----------
stats = source.agg(F.count("*").alias("rows"),
                   F.countDistinct("_source_file").alias("source_files")).first().asDict()
if not stats["rows"]:
    raise AssertionError("The selected Bronze scope is empty")
expected_files = int(params["expected_source_files"])
if expected_files < 0 or (expected_files and stats["source_files"] != expected_files):
    raise AssertionError(f"Expected source files={expected_files}; observed={stats['source_files']}")
duplicate_stats = check_duplicate_content(source)
print(json.dumps({"bronze_version": version, **stats, **duplicate_stats}, indent=2))
dbutils.jobs.taskValues.set(key="bronze_version", value=int(version))
dbutils.notebook.exit(json.dumps({"status": "passed", "bronze_version": version,
                                  **stats, **duplicate_stats}))
