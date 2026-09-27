# Databricks notebook source
# MAGIC %md
# MAGIC # Read-only pilot performance evidence
# MAGIC Run manually after each completed `dev` or `day_test` Job. This is **not** a fourth
# MAGIC Job task or a pipeline library. Set the pilot pipeline ID and that run's
# MAGIC update ID from the pipeline page. No pipeline is started by this notebook.
# MAGIC Export the displayed results with the Job run's task durations.

# COMMAND ----------
import json
import re
from uuid import UUID
from pyspark.sql import functions as F, types as T

for name, default in {"pipeline_id": "", "update_id": "", "job_run_id": "",
                      "catalog": "github_lakehouse", "silver_schema": "silver_day_test"}.items():
    dbutils.widgets.text(name, default)
if not dbutils.widgets.get("pipeline_id") or not dbutils.widgets.get("update_id"):
    raise ValueError("Fill in pipeline_id and update_id, select the matching silver_schema, then Run all again")
pipeline_id = str(UUID(dbutils.widgets.get("pipeline_id")))
update_id = str(UUID(dbutils.widgets.get("update_id")))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("silver_schema")
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", x) for x in (catalog, schema)):
    raise ValueError("Use unquoted catalog and schema identifiers")
spark.conf.set("spark.sql.session.timeZone", "UTC")

events = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")
# Some event-log surfaces expose origin as JSON, others as a struct.
if isinstance(events.schema["origin"].dataType, T.StringType):
    events = events.withColumn("origin", F.from_json("origin", "update_id STRING, flow_name STRING"))
events = events.where(F.col("origin.update_id") == update_id)
if not events.limit(1).count():
    raise ValueError("No visible events for this pipeline/update pair; verify IDs and event-log access")

# COMMAND ----------
# Optional Job metadata uses your notebook identity; no token or agent access is needed.
job_run_id = dbutils.widgets.get("job_run_id")
if job_run_id:
    try:
        from databricks.sdk import WorkspaceClient
        run = WorkspaceClient().jobs.get_run(run_id=int(job_run_id)).as_dict()
        fields = ("run_id", "task_key", "start_time", "end_time", "run_duration", "setup_duration",
                  "execution_duration", "cleanup_duration", "queue_duration", "attempt_number", "state")
        print(json.dumps({"job": {k: run[k] for k in fields if k in run},
                          "tasks": [{k: task[k] for k in fields if k in task} for task in run.get("tasks", [])],
                          "units": "API timestamps and durations are milliseconds"}, indent=2))
    except Exception as exc:
        print(f"Job API metrics unavailable ({type(exc).__name__}); copy durations from the Job run page.")
else:
    print("Job API metrics unavailable: job_run_id was not supplied; use the Job run page.")

# COMMAND ----------
# MAGIC %md
# MAGIC Update and flow timelines. Use the Job UI for total/task durations including
# MAGIC setup and queuing; notebook `audit_elapsed_seconds` excludes compute startup.

# COMMAND ----------
display(events.where("event_type IN ('create_update', 'update_progress', 'flow_progress')")
        .selectExpr("timestamp", "event_type", "origin.flow_name AS flow_name",
                    "details:update_progress.state AS update_state",
                    "details:flow_progress.status AS flow_status",
                    "try_cast(details:flow_progress.metrics.num_output_rows AS BIGINT) AS output_rows",
                    "details:flow_progress.metrics AS metrics",
                    "details:flow_progress.data_quality AS data_quality")
        .orderBy("timestamp", "flow_name"))

# COMMAND ----------
# MAGIC %md
# MAGIC Plans and stream details help investigate source reads, event-type filters,
# MAGIC JSON projections, and AUTO CDC. Preserve each metric's flow and timestamp.
# MAGIC Do not sum successive cumulative snapshots or equate output rows with
# MAGIC distinct final events. Missing byte/shuffle metrics mean unavailable, not zero.

# COMMAND ----------
display(events.where("event_type = 'flow_definition'")
        .selectExpr("timestamp", "origin.flow_name AS flow_name",
                    "details:flow_definition.input_datasets AS inputs",
                    "details:flow_definition.output_dataset AS output",
                    "details:flow_definition.flow_type AS flow_type",
                    "details:flow_definition.explain_text AS explain_text"))
display(events.where("event_type IN ('stream_progress', 'operation_progress', 'runtime_details')")
        .select("timestamp", "event_type", "origin", "details").orderBy("timestamp"))

# COMMAND ----------
# MAGIC %md
# MAGIC Current public-table storage, not bytes written by this update. Private
# MAGIC router storage must come from the pipeline UI/metrics if exposed. Its
# MAGIC expected first-run rows are retained plus unroutable Bronze occurrences;
# MAGIC report an observed router metric separately from that expectation.

# COMMAND ----------
storage = []
for table in ["push_events", "pull_request_events", "issue_events", "issue_comment_events",
              "pull_request_review_events", "pull_request_review_comment_events", "watch_events",
              "fork_events", "release_events", "github_events_quarantine"]:
    table_name = f"{catalog}.{schema}.{table}"
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
            "numFiles", "sizeInBytes", "partitionColumns").first().asDict()
        storage.append({"table": table_name, **row})
    except Exception as exc:
        storage.append({"table": table_name, "status": "unavailable", "error_type": type(exc).__name__})
print(json.dumps({"pipeline_id": pipeline_id, "update_id": update_id, "current_table_storage": storage,
                  "private_router_storage": "Unavailable through public table names; inspect the pipeline UI if exposed",
                  "source_bytes_target_bytes_shuffle": "Use exported metrics when present; absent fields are unavailable",
                  "optimization_assessment": "Requires review of observed metrics and plans; no automatic tuning"}, indent=2))
