"""Focused tests for pilot evidence; the locked contract is unchanged."""
from copy import deepcopy

import pytest
from pyspark.sql import functions as F

from test_silver import bronze, event, valid_events, spark
from silver_contract import QUARANTINE_COLUMNS, candidates, output_columns, route
from silver_pilot_audit import (compare_pilot_reports, pilot_reconciliation, quarantine_profile,
                                semantic_profile, source_inventory)
from silver_validation import check_duplicate_content


@pytest.mark.parametrize("kind,index,prefix", [("PullRequestEvent", 1, "pr"), ("IssuesEvent", 2, "issue")])
def test_semantics_measure_without_rejecting(spark, kind, index, prefix):
    record = deepcopy(valid_events()[index])
    record["payload"]["action"] = "closed"
    obj = record["payload"]["pull_request" if prefix == "pr" else "issue"]
    obj["created_at"] = "2024-02-02T00:00:00Z"
    record["actor"]["id"] = 0
    record["repo"]["id"] = -1
    parsed = candidates(route(bronze(spark, [record])), kind)
    assert parsed.first().failure_reasons == []
    report = semantic_profile(parsed.select(*output_columns(kind)), kind)
    for name in [f"{prefix}_closed_without_closed_at", f"{prefix}_created_after_event", "actor_id_zero", "repo_id_negative"]:
        assert report["checks"][name]["count"] == 1
        assert report["checks"][name]["examples"][0]["event_id"] == "1"
    assert report["closed_at_by_action"]["closed"] == {"rows": 1, "null": 1, "present": 0}
    assert report["closed_at_by_action"]["reopened"]["rows"] == 0
    assert not report["contract_changed"]


def test_release_semantics_and_empty_population(spark):
    missing = valid_events()[-1]
    future = deepcopy(missing)
    future["id"] = "2"
    future["payload"]["release"]["published_at"] = "2024-02-02T00:00:00Z"
    df = candidates(route(bronze(spark, [missing, future])), "ReleaseEvent").select(*output_columns("ReleaseEvent"))
    report = semantic_profile(df, "ReleaseEvent")
    assert report["checks"]["release_missing_published_at"]["count"] == 1
    assert report["checks"]["release_published_after_event"]["count"] == 1
    assert all(r["count"] == 0 for r in semantic_profile(df.limit(0), "ReleaseEvent")["checks"].values())


def test_exact_day_inventory(spark):
    source = (bronze(spark, [event("ForkEvent")]).crossJoin(spark.range(24))
              .withColumn("_source_file_name", F.concat(F.lit("2024-02-01-"), F.col("id"), F.lit(".json.gz")))
              .withColumn("_source_file", F.concat(F.lit("s3://archive/"), F.col("_source_file_name"))).drop("id"))
    report = source_inventory(source, "2024-02-01", "2024-02-01")
    assert report["source_files"] == 24 and report["retained_bronze_rows"] == 24
    wrong = source.withColumn("_source_file_name", F.regexp_replace("_source_file_name", "-23\\.", "-24."))
    with pytest.raises(AssertionError, match="Expected hourly files"):
        source_inventory(wrong, "2024-02-01", "2024-02-01")
    unknown = source.withColumn("raw_json", F.lit('{"type":"FutureEvent"}'))
    assert source_inventory(unknown)["unexpected_type_rows"] == 24
    hour = source.filter(F.col("_source_file_name") == "2024-02-01-0.json.gz")
    assert source_inventory(hour, file_name="2024-02-01-0.json.gz")["source_files"] == 1


def test_quarantine_all_reasons_and_equations(spark):
    source = bronze(spark, [event("PushEvent", size="bad", distinct_size=1)])
    parsed = candidates(route(source), "PushEvent").localCheckpoint(eager=True)
    q = parsed.select(*QUARANTINE_COLUMNS)
    report = quarantine_profile(q)
    assert report["rows"] == 1
    assert any(r["reason"] == "INVALID_CAST" and r["count"] == 1 for r in report["by_all_reasons"])
    assert len(report["by_all_reasons"]) > 1
    inventory = source_inventory(source)
    result = pilot_reconciliation(inventory, report, [])
    assert result["totals"]["quarantined_retained_occurrences"] == 1
    assert len(result["tables"]) == 9
    with pytest.raises(AssertionError, match="Candidate/quarantine"):
        pilot_reconciliation(inventory, {"by_primary_reason": []}, [])
    first = {"status": "passed", "bronze_version": 1, "quarantine_rows": 1,
             "tables": [], "pilot_report": {"configuration": {"silver_schema": "silver_dev"},
                                               "quarantine": report, "reconciliation": result}}
    second = deepcopy(first)
    assert compare_pilot_reports(first, second)["counts_and_quality_unchanged"]
    second["quarantine_rows"] = 2
    with pytest.raises(AssertionError, match="changed between runs"):
        compare_pilot_reports(first, second)
    second["bronze_version"] = 2
    with pytest.raises(AssertionError, match="Bronze changed"):
        compare_pilot_reports(first, second)


def test_duplicate_winner_evidence(spark):
    record = event("PushEvent", size=1, distinct_size=1)
    source = bronze(spark, [record], file="a").unionByName(bronze(spark, [record], file="z"))
    report = check_duplicate_content(source, include_evidence=True)
    assert report["duplicate_ids"] == 1 and report["extra_rows"] == 1
    assert report["winning_lineage_examples"][0]["winning_lineage"]["_source_file"] == "z"
    assert report["raw_payload_conflicts"] == 0 and report["projected_value_conflicts"] == 0
    assert not report["winning_lineage_examples_truncated"]
