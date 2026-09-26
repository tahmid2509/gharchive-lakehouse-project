# Databricks notebook source
# MAGIC %md
# MAGIC # Silver validation
# MAGIC Job task 3. Fails on schema/quality/coverage/dedup errors. Warning-only
# MAGIC anomalies are reported. Keep Bronze quiescent for the duration of this Job.

# COMMAND ----------
import json
import sys
from functools import reduce

for name, default in {
    "source_path": "", "bronze_table": "github_lakehouse.bronze.github_events_raw",
    "catalog": "github_lakehouse", "silver_schema": "silver",
    "source_start_date": "", "source_end_date": "", "source_file_name": "",
    "bronze_version": "", "max_quarantine_rows": "-1",
}.items():
    dbutils.widgets.text(name, default)
params = {name: dbutils.widgets.get(name) for name in [
    "source_path", "bronze_table", "catalog", "silver_schema", "source_start_date",
    "source_end_date", "source_file_name", "bronze_version", "max_quarantine_rows",
]}
if params["source_path"]:
    sys.path.insert(0, params["source_path"])
from pyspark.sql import functions as F
from silver_contract import EVENTS, LINEAGE, identifier, scoped_source
from silver_validation import check_quarantine, check_table, reconcile

spark.conf.set("spark.sql.session.timeZone", "UTC")
bronze_table = identifier(params["bronze_table"], 3)
catalog = identifier(params["catalog"])
schema = identifier(params["silver_schema"])
if not params["bronze_version"]:
    raise ValueError("Run through the Job, or supply the audited Bronze version explicitly")
version = int(params["bronze_version"])
latest = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").first()["version"]
if latest != version:
    raise AssertionError("Bronze changed during the Job. Finish ingestion, then rerun the Job.")
source = scoped_source(spark.read.option("versionAsOf", version).table(bronze_table),
                       params["source_start_date"], params["source_end_date"], params["source_file_name"])
quarantine = spark.read.table(f"{catalog}.{schema}.github_events_quarantine")

# COMMAND ----------
check_quarantine(quarantine)
quarantine_count = quarantine.count()
limit = int(params["max_quarantine_rows"])
if limit < -1 or (limit >= 0 and quarantine_count > limit):
    raise AssertionError(f"Quarantine rows={quarantine_count}; allowed={limit}")
print("Quarantine by primary reason:")
quarantine.groupBy("event_type", "failure_reason").count().show(truncate=False)

table_reports = []
key_frames = []
for event_type, (table_name, _) in EVENTS.items():
    table = spark.read.table(f"{catalog}.{schema}.{table_name}")
    report = check_table(table, event_type)
    table_reports.append(report)
    key_frames.append(table.select("event_id", "event_type", *LINEAGE))
    print(json.dumps(report, default=str))

actual_keys = reduce(lambda left, right: left.unionByName(right), key_frames)
reconciliation = reconcile(source, quarantine, actual_keys)
if spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").first()["version"] != version:
    raise AssertionError("Bronze changed while validation was running; rerun with ingestion stopped")
summary = {"status": "passed", "bronze_version": version, "quarantine_rows": quarantine_count,
           "tables": table_reports, "reconciliation": reconciliation}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
