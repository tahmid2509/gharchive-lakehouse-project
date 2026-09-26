"""Run with pytest on Spark 3.5+/4.x (Java required); no Databricks credentials."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession, functions as F, Window

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from silver_contract import (EVENTS, LINEAGE, QUARANTINE_COLUMNS, candidates, input_schema,
                             output_columns, output_types, route, scoped_source, unroutable)
from silver_validation import check_duplicate_content, check_quarantine, check_table, reconcile


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("silver-contract-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def event(kind, key="1", **payload):
    return {"id": key, "type": kind, "created_at": "2024-02-01T00:00:00Z",
            "actor": {"id": 11, "login": "MixedCaseActor"},
            "repo": {"id": 22, "name": "Owner/Repository"}, "payload": payload}


def bronze(spark, records, file="a", ingested="2026-09-26 00:00:00"):
    # JVM-only literals: tests work without Python worker processes or local files.
    values = [json.dumps(r) if isinstance(r, dict) else r for r in records]
    df = spark.range(1).select(F.explode(F.array(*[F.lit(v) for v in values])).alias("raw_json"))
    return (df.withColumn("_source_file", F.lit(file)).withColumn("_source_file_name", F.lit(file))
            .withColumn("_source_date", F.lit("2024-02-01").cast("date"))
            .withColumn("_ingested_at", F.lit(ingested).cast("timestamp")))


def valid_events():
    return [
        event("PushEvent", size=3, distinct_size=2, commits=[{"sha": "a"}, {"sha": "b"}]),
        event("PullRequestEvent", action="opened", number=7, pull_request={
            "id": 30, "number": 999, "created_at": "2024-01-01T00:00:00Z", "closed_at": None,
            "merged_at": None, "merged": False, "commits": 2, "additions": 4,
            "deletions": 1, "changed_files": 2}),
        event("IssuesEvent", action="opened", issue={"id": 40, "number": 5,
              "created_at": "2024-01-01T00:00:00Z", "closed_at": None}),
        event("IssueCommentEvent", action="created", issue={"id": 40, "number": 5}, comment={"id": 50}),
        event("PullRequestReviewEvent", action="created", pull_request={"id": 30, "number": 7},
              review={"id": 60, "state": "pending"}),
        event("PullRequestReviewCommentEvent", action="created", pull_request={"id": 30, "number": 7},
              comment={"id": 70}),
        event("WatchEvent", action="started"),
        event("ForkEvent", forkee={"unused": [1, 2, 3]}),
        event("ReleaseEvent", action="published", release={"id": 80, "tag_name": "  ",
              "published_at": None, "draft": False, "prerelease": True}),
    ]


@pytest.mark.parametrize("record", valid_events(), ids=list(EVENTS))
def test_nine_contracts(spark, record):
    kind = record["type"]
    candidate = candidates(route(bronze(spark, [record])), kind)
    row = candidate.first()
    assert row.failure_reasons == []
    final = candidate.select(*output_columns(kind))
    assert {f.name: f.dataType.simpleString() for f in final.schema} == output_types(kind)
    assert check_table(final, kind)["rows"] == 1
    assert row.actor_login == "MixedCaseActor" and row.repo_name == "Owner/Repository"
    if kind == "PullRequestEvent":
        assert row.pr_number == 7 and row.pr_closed_at is None and row.pr_merged_at is None
    if kind == "IssueCommentEvent":
        assert row.comment_target_type == "issue"
    if kind == "ReleaseEvent":
        assert row.release_tag is None and row.release_published_at is None
    assert "commits" not in input_schema(kind).simpleString() or kind == "PullRequestEvent"


def test_issue_comment_and_malformed_routing(spark):
    pr = event("IssueCommentEvent", "2", action="created", issue={"id": 40, "number": 5,
               "pull_request": {"url": "https://example.test/pr/5"}}, comment={"id": 50})
    source = bronze(spark, [pr, event("IssueCommentEvent", "3"), "{bad json", event("CreateEvent", "4")])
    routed = route(source)
    assert routed.count() == 3
    rows = {r.event_id: r for r in candidates(routed, "IssueCommentEvent").collect()}
    assert rows["2"].comment_target_type == "pull_request"
    assert rows["3"].failure_reason == "MISSING_ISSUE_COMMENT_PAYLOAD"
    assert unroutable(routed).first().failure_reason == "MALFORMED_JSON"


def test_invalid_optional_cast_and_lifecycle(spark):
    pr = deepcopy(valid_events()[1])
    cases = []
    for key, edits in [("bad_cast", {"closed_at": "not-a-time"}),
                       ("bad_merge", {"merged": True}),
                       ("negative", {"commits": -1}),
                       ("bad_time", {"closed_at": "2023-01-01T00:00:00Z"})]:
        item = deepcopy(pr)
        item["id"] = key
        item["payload"]["pull_request"].update(edits)
        cases.append(item)
    rows = {r.event_id: r.failure_reasons for r in candidates(route(bronze(spark, cases)), "PullRequestEvent").collect()}
    assert "INVALID_CAST" in rows["bad_cast"]
    assert "PR_MERGED_TIMESTAMP_REQUIRED" in rows["bad_merge"]
    assert "NONNEGATIVE_PR_COUNTS" in rows["negative"]
    assert "PR_CLOSED_BEFORE_CREATED" in rows["bad_time"]


def test_warnings_keep_rows_and_utc(spark):
    push = event("PushEvent", size=1, distinct_size=2)
    push["created_at"] = "2024-02-01T23:30:00-02:00"
    result = candidates(route(bronze(spark, [push])), "PushEvent").select(*output_columns("PushEvent"))
    row = result.first()
    assert str(row.event_date) == "2024-02-02" and row.push_distinct_commit_count == 2
    report = check_table(result, "PushEvent")
    assert report["warn_distinct_not_above_total"] == 1
    assert report["warn_source_date_matches_event"] == 1
    future = event("WatchEvent", action="future_valid_action")
    watch = candidates(route(bronze(spark, [future])), "WatchEvent").select(*output_columns("WatchEvent"))
    assert check_table(watch, "WatchEvent")["warn_observed_action"] == 1


def test_missing_common_and_bad_numeric(spark):
    record = event("PushEvent", size="garbage", distinct_size=1)
    record["actor"]["login"] = " "
    record["repo"]["id"] = "9223372036854775808"
    row = candidates(route(bronze(spark, [record])), "PushEvent").first()
    assert "INVALID_CAST" in row.failure_reasons
    assert "MISSING_COMMON_REQUIRED_FIELD" in row.failure_reasons


def test_reconciliation_duplicate_overlap_and_winner(spark):
    good = event("IssueCommentEvent", "ok", action="created", issue={"id": 40, "number": 5}, comment={"id": 50})
    bad = event("IssueCommentEvent", "bad")
    source = bronze(spark, [good, bad]).unionByName(bronze(spark, [good, bad], file="z"))
    assert check_duplicate_content(source) == {"duplicate_ids": 2, "extra_rows": 2}
    # Production validation reads persisted Delta outputs. Truncate this fixture's
    # parsing lineage to model that boundary and avoid recursively inlining it.
    parsed = candidates(route(source), "IssueCommentEvent").localCheckpoint(eager=True)
    q = parsed.filter("size(failure_reasons) > 0").select(*QUARANTINE_COLUMNS)
    check_quarantine(q)
    winner = (parsed.filter("size(failure_reasons) = 0")
              .withColumn("rn", F.row_number().over(Window.partitionBy("event_id")
                         .orderBy(F.desc("_ingested_at"), F.desc("_source_file"))))
              .filter("rn = 1").select("event_id", "event_type", *LINEAGE))
    report = reconcile(source, q, winner)
    assert report == [{"event_type": "IssueCommentEvent", "valid_bronze_rows": 2,
                       "silver_rows": 1, "duplicates_removed": 1}]
    with pytest.raises(AssertionError, match="uniqueness"):
        reconcile(source, q, winner.unionByName(winner))
    with pytest.raises(AssertionError, match="lineage"):
        reconcile(source, q, winner.withColumn("_source_file", F.lit("a")))


def test_conflicting_duplicate_fails_preflight(spark):
    a = event("PushEvent", size=1, distinct_size=1)
    b = event("PushEvent", size=2, distinct_size=1)
    with pytest.raises(AssertionError, match="Conflicting raw JSON"):
        check_duplicate_content(bronze(spark, [a, b]))
    with pytest.raises(AssertionError, match="Cross-type"):
        check_duplicate_content(bronze(spark, [a, event("WatchEvent", action="started")]))


def test_scope_filters_before_routing(spark):
    source = bronze(spark, [event("ForkEvent")], file="one-hour.json.gz")
    assert scoped_source(source, "2024-02-01", "2024-02-01", "one-hour.json.gz").count() == 1
    assert scoped_source(source, "2024-02-02").count() == 0
    with pytest.raises(ValueError):
        scoped_source(source, "2024-02-03", "2024-02-01")


def test_python_and_deployment_graph():
    import yaml
    for path in ROOT.glob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    config = yaml.safe_load((ROOT / "databricks.yml").read_text())
    pipeline = config["resources"]["pipelines"]["silver_pipeline"]
    job = config["resources"]["jobs"]["silver_job"]
    assert pipeline["serverless"] is True and pipeline["continuous"] is False
    assert len(pipeline["libraries"]) == 1
    assert job["tasks"][1]["depends_on"] == [{"task_key": "preflight"}]
    assert job["tasks"][2]["depends_on"] == [{"task_key": "silver_pipeline"}]
    assert job["tasks"][1]["pipeline_task"]["full_refresh"] is False
    for task in job["tasks"]:
        if "notebook_task" in task:
            assert (ROOT / task["notebook_task"]["notebook_path"]).exists()
    assert config["targets"]["dev"]["variables"]["silver_schema"] == "silver_dev"
