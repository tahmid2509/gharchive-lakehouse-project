# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Lakeflow pipeline
# MAGIC Add this notebook as the **only pipeline library**. The other numbered
# MAGIC notebooks are Job tasks. See README.md for deployment and configuration.

# COMMAND ----------
import sys
sys.path.insert(0, spark.conf.get("silver.source_path"))
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from silver_contract import (
    EVENTS, LINEAGE, QUARANTINE_COLUMNS, candidates, identifier, output_columns,
    route, scoped_source, unroutable, warning_rules,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
BRONZE = identifier(spark.conf.get("silver.bronze_table"), 3)
START = spark.conf.get("silver.source_start_date", "")
END = spark.conf.get("silver.source_end_date", "")
FILE = spark.conf.get("silver.source_file_name", "")
MAX_BYTES = spark.conf.get("silver.max_bytes_per_trigger", "1g")

# COMMAND ----------
@dp.table(name="retained_event_router", private=True, partition_cols=["event_type"],
          comment="Private append-only router; one Bronze source read, no payload flattening.")
def retained_event_router():
    source = spark.readStream.option("maxBytesPerTrigger", MAX_BYTES).table(BRONZE)
    return route(scoped_source(source, START, END, FILE))


dp.create_streaming_table(
    "github_events_quarantine",
    comment="Contract-breaking Bronze occurrences; source duplicates are preserved for reconciliation.",
    expect_all_or_fail={"reason_present": "failure_reason IS NOT NULL"},
)


@dp.append_flow(target="github_events_quarantine", name="quarantine_unroutable")
def quarantine_unroutable():
    return unroutable(spark.readStream.table("retained_event_router"))

# COMMAND ----------
def register_event(event_type, table_name):
    """Bind each loop value in its own scope: every definition has a stable name."""
    candidate_name = f"{table_name}_candidates"
    valid_name = f"{table_name}_valid"

    @dp.temporary_view(name=candidate_name)
    def event_candidates():
        return candidates(spark.readStream.table("retained_event_router"), event_type)

    @dp.temporary_view(name=valid_name)
    def valid_candidates():
        return (spark.readStream.table(candidate_name)
                .filter(F.size("failure_reasons") == 0)
                .select(*output_columns(event_type)))

    @dp.append_flow(target="github_events_quarantine", name=f"quarantine_{table_name}")
    def invalid_candidates():
        return (spark.readStream.table(candidate_name)
                .filter(F.size("failure_reasons") > 0)
                .withColumn("event_type", F.lit(event_type))
                .withColumn("event_id", F.get_json_object("raw_json", "$.id"))
                .select(*QUARANTINE_COLUMNS))

    # Infer target types from the explicit typed projection. No SCD2 history columns.
    dp.create_streaming_table(
        table_name, comment=f"One clean {event_type} per event_id; UTC timestamps.",
        expect_all=warning_rules(event_type),
        expect_all_or_fail={"event_type_purity": f"event_type = '{event_type}'",
                            "event_key_present": "event_id IS NOT NULL"},
    )
    dp.create_auto_cdc_flow(
        name=f"upsert_{table_name}", target=table_name, source=valid_name,
        keys=["event_id"], sequence_by=F.struct("_ingested_at", "_source_file"),
        column_list=output_columns(event_type), stored_as_scd_type=1,
        ignore_null_updates=False,
    )


for event_type, (table_name, _) in EVENTS.items():
    register_event(event_type, table_name)
