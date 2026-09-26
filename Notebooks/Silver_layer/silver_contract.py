"""Phase 2 contracts and stateless Spark transformations shared by the pipeline/tests.

Field mappings are taken from 05_silver_data_profiling. Input JSON leaves are
strings so a failed optional timestamp cast cannot masquerade as a source NULL.
No Spark actions, writes, UDFs, array explosions, or deduplication happen here.
"""
from datetime import date
from functools import reduce
import re

from pyspark.sql import functions as F, types as T

COMMON = [
    ("event_id", "id", "STRING"),
    ("event_type", "type", "STRING"),
    ("event_timestamp", "created_at", "TIMESTAMP"),
    ("actor_id", "actor.id", "BIGINT"),
    ("actor_login", "actor.login", "STRING"),
    ("repo_id", "repo.id", "BIGINT"),
    ("repo_name", "repo.name", "STRING"),
]
LINEAGE = ["_source_file", "_source_date", "_ingested_at"]
COMMON_COLUMNS = [
    "event_id", "event_type", "event_timestamp", "event_date", "actor_id",
    "actor_login", "repo_id", "repo_name", *LINEAGE,
]
# Each tuple is (published name, exact source path, published SQL type).
EVENTS = {
    "PushEvent": ("push_events", [
        ("push_commit_count", "payload.size", "BIGINT"),
        ("push_distinct_commit_count", "payload.distinct_size", "BIGINT"),
    ]),
    "PullRequestEvent": ("pull_request_events", [
        ("pr_action", "payload.action", "STRING"),
        ("pr_number", "payload.number", "BIGINT"),
        ("pr_id", "payload.pull_request.id", "BIGINT"),
        ("pr_created_at", "payload.pull_request.created_at", "TIMESTAMP"),
        ("pr_closed_at", "payload.pull_request.closed_at", "TIMESTAMP"),
        ("pr_merged_at", "payload.pull_request.merged_at", "TIMESTAMP"),
        ("pr_merged", "payload.pull_request.merged", "BOOLEAN"),
        ("pr_commits", "payload.pull_request.commits", "BIGINT"),
        ("pr_additions", "payload.pull_request.additions", "BIGINT"),
        ("pr_deletions", "payload.pull_request.deletions", "BIGINT"),
        ("pr_changed_files", "payload.pull_request.changed_files", "BIGINT"),
    ]),
    "IssuesEvent": ("issue_events", [
        ("issue_action", "payload.action", "STRING"),
        ("issue_id", "payload.issue.id", "BIGINT"),
        ("issue_number", "payload.issue.number", "BIGINT"),
        ("issue_created_at", "payload.issue.created_at", "TIMESTAMP"),
        ("issue_closed_at", "payload.issue.closed_at", "TIMESTAMP"),
    ]),
    "IssueCommentEvent": ("issue_comment_events", [
        ("comment_action", "payload.action", "STRING"),
        ("issue_id", "payload.issue.id", "BIGINT"),
        ("issue_number", "payload.issue.number", "BIGINT"),
        ("comment_target_type", None, "STRING"),
        ("comment_id", "payload.comment.id", "BIGINT"),
    ]),
    "PullRequestReviewEvent": ("pull_request_review_events", [
        ("review_action", "payload.action", "STRING"),
        ("pr_id", "payload.pull_request.id", "BIGINT"),
        ("pr_number", "payload.pull_request.number", "BIGINT"),
        ("review_id", "payload.review.id", "BIGINT"),
        ("review_state", "payload.review.state", "STRING"),
    ]),
    "PullRequestReviewCommentEvent": ("pull_request_review_comment_events", [
        ("comment_action", "payload.action", "STRING"),
        ("pr_id", "payload.pull_request.id", "BIGINT"),
        ("pr_number", "payload.pull_request.number", "BIGINT"),
        ("comment_id", "payload.comment.id", "BIGINT"),
    ]),
    "WatchEvent": ("watch_events", [("watch_action", "payload.action", "STRING")]),
    "ForkEvent": ("fork_events", []),
    "ReleaseEvent": ("release_events", [
        ("release_action", "payload.action", "STRING"),
        ("release_id", "payload.release.id", "BIGINT"),
        ("release_tag", "payload.release.tag_name", "STRING"),
        ("release_published_at", "payload.release.published_at", "TIMESTAMP"),
        ("release_draft", "payload.release.draft", "BOOLEAN"),
        ("release_prerelease", "payload.release.prerelease", "BOOLEAN"),
    ]),
}
NULLABLE = {
    "pr_closed_at", "pr_merged_at", "issue_closed_at", "release_tag",
    "release_published_at",
}
EXCLUDED = ["CommitCommentEvent", "CreateEvent", "DeleteEvent", "GollumEvent",
            "MemberEvent", "PublicEvent"]
QUARANTINE_COLUMNS = ["raw_json", "event_id", "event_type", *LINEAGE,
                      "failure_reason", "failure_reasons"]


def identifier(value, parts=1):
    """Validate identifiers before interpolating table names into SQL."""
    if len(value.split(".")) != parts or not all(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in value.split(".")
    ):
        raise ValueError(f"Expected a {parts}-part unquoted identifier: {value!r}")
    return value


def scoped_source(df, start="", end="", file_name=""):
    """Apply the same optional development scope to pipeline and audit reads."""
    if start:
        date.fromisoformat(start)
        df = df.filter(F.col("_source_date") >= F.lit(start).cast("date"))
    if end:
        date.fromisoformat(end)
        df = df.filter(F.col("_source_date") <= F.lit(end).cast("date"))
    if start and end and start > end:
        raise ValueError("source_start_date must be <= source_end_date")
    if file_name:
        df = df.filter(F.col("_source_file_name") == file_name)
    return df


def route(df):
    # A missing/blank type must reach quarantine, not disappear in an isin filter.
    df = df.select("raw_json", *LINEAGE).withColumn(
        "event_type", F.get_json_object("raw_json", "$.type")
    )
    return df.filter(F.col("event_type").isin(list(EVENTS)) |
                     F.col("event_type").isNull() | (F.trim("event_type") == ""))


def input_schema(event_type):
    paths = [path for _, path, _ in COMMON + EVENTS[event_type][1] if path]
    if event_type == "IssueCommentEvent":
        paths.append("payload.issue.pull_request.url")
    root = {}
    for path in paths:
        node = root
        keys = path.split(".")
        for key in keys[:-1]:
            node = node.setdefault(key, {})
        node[keys[-1]] = None

    def struct(node):
        return T.StructType([T.StructField(k, struct(v) if isinstance(v, dict)
                                          else T.StringType(), True)
                             for k, v in node.items()])
    return struct(root).add("_corrupt_record", T.StringType())


def output_columns(event_type):
    return COMMON_COLUMNS + [name for name, _, _ in EVENTS[event_type][1]]


def output_types(event_type):
    types = {name: dtype.lower() for name, _, dtype in COMMON + EVENTS[event_type][1]}
    types.update(event_date="date", _source_file="string", _source_date="date",
                 _ingested_at="timestamp")
    return types


def required_columns(event_type):
    return [name for name in output_columns(event_type) if name not in NULLABLE]


def any_of(expressions):
    return reduce(lambda a, b: a | b, expressions, F.lit(False))


def missing(name, dtype):
    return F.col(name).isNull() | ((F.trim(F.col(name)) == "")
                                 if dtype == "string" else F.lit(False))


def hard_rules(event_type):
    """Named SQL PASS conditions. NULL-safe branches preserve legitimate NULLs."""
    rules = {}
    if event_type == "PushEvent":
        rules["NONNEGATIVE_PUSH_COUNTS"] = (
            "push_commit_count >= 0 AND push_distinct_commit_count >= 0")
    if event_type == "PullRequestEvent":
        rules.update({
            "NONNEGATIVE_PR_COUNTS": "pr_commits >= 0 AND pr_additions >= 0 "
                                    "AND pr_deletions >= 0 AND pr_changed_files >= 0",
            "PR_MERGED_TIMESTAMP_REQUIRED": "NOT pr_merged OR pr_merged_at IS NOT NULL",
            "PR_CLOSED_BEFORE_CREATED": "pr_closed_at IS NULL OR pr_closed_at >= pr_created_at",
            "PR_MERGED_BEFORE_CREATED": "pr_merged_at IS NULL OR pr_merged_at >= pr_created_at",
        })
    if event_type == "IssuesEvent":
        rules["ISSUE_CLOSED_BEFORE_CREATED"] = (
            "issue_closed_at IS NULL OR issue_closed_at >= issue_created_at")
    return rules


def warning_rules(event_type):
    rules = {"source_date_matches_event": "event_date = _source_date"}
    observed = {
        "PullRequestEvent": ("pr_action", "'opened','closed','reopened'"),
        "IssuesEvent": ("issue_action", "'opened','closed','reopened'"),
        "IssueCommentEvent": ("comment_action", "'created'"),
        "PullRequestReviewEvent": ("review_action", "'created'"),
        "PullRequestReviewCommentEvent": ("comment_action", "'created'"),
        "WatchEvent": ("watch_action", "'started'"),
        "ReleaseEvent": ("release_action", "'published'"),
    }
    if event_type in observed:
        name, values = observed[event_type]
        rules["observed_action"] = f"{name} IN ({values})"
    if event_type == "PushEvent":
        rules["distinct_not_above_total"] = "push_distinct_commit_count <= push_commit_count"
    if event_type == "PullRequestEvent":
        rules["unmerged_has_no_merge_time"] = "pr_merged OR pr_merged_at IS NULL"
    if event_type == "PullRequestReviewEvent":
        rules["observed_review_state"] = (
            "review_state IN ('approved','commented','changes_requested','dismissed','pending')")
    if event_type == "ReleaseEvent":
        rules["published_has_anchor"] = (
            "release_action <> 'published' OR release_published_at IS NOT NULL")
    return rules


def candidates(df, event_type):
    """One source event stays one row; no commits[] expansion is in this contract."""
    mappings = COMMON + EVENTS[event_type][1]
    parsed = df.filter(F.col("event_type") == event_type).withColumn(
        "_json", F.from_json("raw_json", input_schema(event_type),
                             {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})
    )
    raw = parsed.select("raw_json", *LINEAGE,
                        F.col("_json._corrupt_record").alias("_corrupt_record"),
                        *[F.col(f"_json.{path}").alias(f"_raw_{name}")
                          for name, path, _ in mappings if path],
                        *([F.col("_json.payload.issue.pull_request.url").alias("_pr_url")]
                          if event_type == "IssueCommentEvent" else []))
    typed = raw.select("*", *[
        F.expr(f"try_cast(`_raw_{name}` AS {dtype})").alias(name)
        for name, path, dtype in mappings if path
    ]).withColumn("event_date", F.to_date("event_timestamp"))
    if event_type == "IssueCommentEvent":
        typed = typed.withColumn("comment_target_type", F.when(F.col("issue_id").isNull(),
            F.lit(None).cast("string")).when(F.col("_pr_url").isNotNull(), "pull_request")
            .otherwise("issue"))
    if event_type == "ReleaseEvent":
        # Only blanks become NULL; retain the exact spelling of nonblank tags.
        typed = typed.withColumn("release_tag", F.when(F.trim("release_tag") == "",
                                 F.lit(None).cast("string")).otherwise(F.col("release_tag")))
    types = output_types(event_type)
    common_bad = any_of([missing(n, types[n]) for n in COMMON_COLUMNS])
    event_bad = any_of([missing(n, types[n]) for n, _, _ in EVENTS[event_type][1]
                        if n not in NULLABLE])
    cast_bad = any_of([F.col(f"_raw_{n}").isNotNull() & F.col(n).isNull()
                      for n, path, dtype in mappings if path and dtype != "STRING"])
    checks = [
        ("JSON_PARSE_OR_SCHEMA_FAILURE", F.col("_corrupt_record").isNotNull()),
        ("INVALID_CAST", cast_bad),
        ("MISSING_COMMON_REQUIRED_FIELD", common_bad),
        ("MISSING_ISSUE_COMMENT_PAYLOAD" if event_type == "IssueCommentEvent"
         else "MISSING_EVENT_REQUIRED_FIELD", event_bad),
    ]
    checks += [(name, ~F.coalesce(F.expr(expr), F.lit(False)))
               for name, expr in hard_rules(event_type).items()]
    reasons = F.filter(F.array(*[F.when(condition, F.lit(name)) for name, condition in checks]),
                       lambda x: x.isNotNull())
    return (typed.withColumn("failure_reasons", reasons)
            .withColumn("failure_reason", F.try_element_at("failure_reasons", F.lit(1)))
            .select(*output_columns(event_type), "raw_json", "failure_reason", "failure_reasons"))


def unroutable(df):
    reason = F.when(F.get_json_object("raw_json", "$").isNull(), "MALFORMED_JSON")\
              .otherwise("MISSING_EVENT_TYPE")
    return (df.filter(F.col("event_type").isNull() | (F.trim("event_type") == ""))
            .withColumn("event_id", F.get_json_object("raw_json", "$.id"))
            .withColumn("failure_reason", reason)
            .withColumn("failure_reasons", F.array("failure_reason"))
            .select(*QUARANTINE_COLUMNS))


def require_bronze_schema(df):
    expected = {"raw_json": "string", "_source_file": "string", "_source_file_name": "string",
                "_source_date": "date", "_ingested_at": "timestamp"}
    actual = {f.name: f.dataType.simpleString() for f in df.schema}
    if any(actual.get(k) != v for k, v in expected.items()):
        raise ValueError(f"Bronze schema mismatch. Expected {expected}; found {actual}")
