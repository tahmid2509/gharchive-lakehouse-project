"""Batch audits for the Silver Job. Keep Spark actions out of pipeline definitions."""
from functools import reduce

from pyspark.sql import functions as F
from silver_contract import (EVENTS, LINEAGE, candidates, hard_rules, missing,
                             output_columns, output_types, required_columns,
                             scoped_source, unroutable, warning_rules)


def assert_empty(df, message):
    sample = df.limit(5).collect()
    if sample:
        raise AssertionError(f"{message}; sample={sample}")


def occurrence_match(left, right):
    return reduce(lambda a, b: a & b,
                  [F.col(f"{left}.{c}").eqNullSafe(F.col(f"{right}.{c}"))
                   for c in ["raw_json", *LINEAGE]])


def check_duplicate_content(source):
    """Shuffle narrow IDs first; compare full JSON only in duplicated ID groups."""
    envelopes = source.select("raw_json", *LINEAGE, F.get_json_object("raw_json", "$.id").alias("event_id"),
                              F.get_json_object("raw_json", "$.type").alias("event_type"))
    retained = envelopes.filter(F.col("event_type").isin(list(EVENTS)))
    duplicates = (retained.filter(F.col("event_id").isNotNull() & (F.trim("event_id") != "")).groupBy("event_id")
                  .agg(F.count("*").alias("copies"), F.min("event_type").alias("min_type"),
                       F.max("event_type").alias("max_type"))
                  .filter("copies > 1"))
    # The profiled week has only 78 duplicate IDs. Bound driver collection and
    # reuse those IDs so the common case performs the large ID aggregation once.
    sample = duplicates.limit(10001).collect()
    if len(sample) <= 10000:
        if any(r.min_type != r.max_type for r in sample):
            raise AssertionError("Cross-type event_id conflict")
        stats = {"duplicate_ids": len(sample), "extra_rows": sum(r.copies - 1 for r in sample)}
        duplicate_rows = retained.filter(F.col("event_id").isin([r.event_id for r in sample]))
    else:
        assert_empty(duplicates.filter("min_type <> max_type"), "Cross-type event_id conflict")
        stats = duplicates.agg(F.count("*").alias("duplicate_ids"),
                               F.sum(F.col("copies") - 1).alias("extra_rows")).first().asDict()
        duplicate_rows = retained.join(duplicates.select("event_id"), "event_id")
    if stats["duplicate_ids"]:
        conflicts = (duplicate_rows.groupBy("event_id")
                     .agg(F.countDistinct("raw_json").alias("raw_versions"))
                     .filter("raw_versions > 1"))
        assert_empty(conflicts, "Conflicting raw JSON for one event_id; investigate before publishing")
        tied_lineage = (duplicate_rows.groupBy("event_id", "_ingested_at", "_source_file")
                        .agg(F.countDistinct("_source_date").alias("source_dates"))
                        .filter("source_dates > 1"))
        assert_empty(tied_lineage, "Conflicting source_date for equal event_id/sequence")
    return stats


def reconcile(source, quarantine, actual):
    """Prove occurrence coverage, key uniqueness, and deterministic winner lineage.

    Quarantine preserves occurrences. It is normally tiny; automatic broadcast is
    left to Spark so unexpectedly large quarantine is not forced onto executors.
    actual contains only event_id, event_type and lineage from the nine outputs.
    """
    identity = ["raw_json", *LINEAGE]
    q_counts = quarantine.groupBy(*identity).count().withColumnRenamed("count", "q_count")
    matched = source.alias("b").join(q_counts.alias("q"), occurrence_match("b", "q"), "left_semi")
    b_counts = matched.groupBy(*identity).count().withColumnRenamed("count", "b_count")
    membership = q_counts.alias("q").join(b_counts.alias("b"), occurrence_match("q", "b"), "left")
    assert_empty(membership.filter(F.col("b.b_count").isNull() |
                                   (F.col("q.q_count") != F.col("b.b_count")))
                 .select("q.q_count", "b.b_count"),
                 "Quarantine is not an exact set of invalid Bronze occurrences")

    envelopes = source.withColumn("event_type", F.get_json_object("raw_json", "$.type"))
    retained = envelopes.filter(F.col("event_type").isin(list(EVENTS)))
    valid_source = retained.alias("b").join(q_counts.alias("q"), occurrence_match("b", "q"), "left_anti")
    expected = (valid_source.withColumn("event_id", F.get_json_object("raw_json", "$.id"))
                .groupBy("event_type", "event_id")
                .agg(F.count("*").alias("valid_copies"),
                     F.max(F.struct("_ingested_at", "_source_file")).alias("expected_lineage")))
    observed = (actual.groupBy("event_type", "event_id")
                .agg(F.count("*").alias("target_copies"),
                     F.max(F.struct("_ingested_at", "_source_file")).alias("actual_lineage")))
    compared = expected.join(observed, ["event_type", "event_id"], "full")
    invalid = (F.col("valid_copies").isNull() | F.col("target_copies").isNull() |
               (F.col("target_copies") != 1) |
               ~F.col("expected_lineage").eqNullSafe(F.col("actual_lineage")))
    summary = [r.asDict() for r in compared.groupBy("event_type").agg(
        F.sum("valid_copies").alias("valid_bronze_rows"),
        F.count("*").alias("silver_rows"),
        F.sum(F.col("valid_copies") - 1).alias("duplicates_removed"),
        F.count(F.when(invalid, 1)).alias("invalid_keys"),
    ).collect()]
    if any(r["invalid_keys"] for r in summary):
        raise AssertionError(f"Silver key coverage, uniqueness, or deterministic lineage mismatch: {summary}")
    # Missing/unroutable types are outside the retained-key equation.
    untyped = envelopes.filter(F.col("event_type").isNull() | (F.trim("event_type") == ""))
    q_untyped = quarantine.filter(F.col("event_type").isNull() | (F.trim("event_type") == ""))
    if untyped.count() != q_untyped.count():
        raise AssertionError("Unroutable Bronze rows are missing from quarantine")
    return [{k: v for k, v in r.items() if k != "invalid_keys"} for r in summary]


def check_quarantine(quarantine):
    assert_empty(quarantine.filter(F.col("failure_reasons").isNull() |
                 (F.size("failure_reasons") == 0) | ~F.col("failure_reason").eqNullSafe(
                     F.try_element_at("failure_reasons", F.lit(1))))
                 .select("event_id", "failure_reason", "failure_reasons"),
                 "Quarantine reason is missing or inconsistent")
    assert_empty(quarantine.filter(~F.coalesce(F.col("event_type").isin(list(EVENTS)) |
                 F.col("event_type").isNull() | (F.trim("event_type") == ""), F.lit(False))),
                 "Valid out-of-scope event incorrectly quarantined")
    for event_type in EVENTS:
        actual = quarantine.filter(F.col("event_type") == event_type)
        # Preserve the recorded reason while recomputing the classification on the small bad set.
        expected = candidates(actual, event_type).select(
            "raw_json", *LINEAGE, F.col("failure_reasons").alias("expected_reasons"))
        paired = actual.alias("q").join(expected.alias("b"), occurrence_match("q", "b"), "inner")
        assert_empty(paired.filter((F.size("expected_reasons") == 0) |
                     ~F.col("q.failure_reasons").eqNullSafe(F.col("expected_reasons")))
                     .select("q.event_id", "q.failure_reasons", "expected_reasons"),
                     f"Wrong quarantine classification for {event_type}")
    untyped = quarantine.filter(F.col("event_type").isNull() | (F.trim("event_type") == ""))
    recomputed = unroutable(untyped).select("raw_json", *LINEAGE,
                                          F.col("failure_reason").alias("expected_reason"))
    paired = untyped.alias("q").join(recomputed.alias("b"), occurrence_match("q", "b"), "inner")
    assert_empty(paired.filter(~F.col("q.failure_reason").eqNullSafe(F.col("expected_reason")))
                 .select("q.failure_reason", "expected_reason"), "Wrong unroutable classification")


def check_table(df, event_type):
    actual_schema = {f.name: f.dataType.simpleString() for f in df.schema}
    expected_schema = output_types(event_type)
    if actual_schema != expected_schema or df.columns != output_columns(event_type):
        raise AssertionError(f"{event_type} schema mismatch: {actual_schema}; expected {expected_schema}")
    checks = {f"required_{c}": ~missing(c, expected_schema[c]) for c in required_columns(event_type)}
    checks.update({k: F.expr(v) for k, v in hard_rules(event_type).items()})
    checks["correct_type"] = F.col("event_type") == event_type
    checks["correct_event_date"] = F.col("event_date") == F.to_date("event_timestamp")
    warns = warning_rules(event_type)
    row = df.agg(F.count("*").alias("rows"), F.countDistinct("_source_file").alias("source_files"),
                 F.min("_source_date").alias("first_source_date"),
                 F.max("_source_date").alias("last_source_date"),
                 *[F.count(F.when(~F.coalesce(v, F.lit(False)), 1)).alias(k)
                   for k, v in checks.items()],
                 *[F.count(F.when(~F.coalesce(F.expr(v), F.lit(False)), 1)).alias(f"warn_{k}")
                   for k, v in warns.items()]).first().asDict()
    failures = {k: row[k] for k in checks if row[k]}
    if failures:
        raise AssertionError(f"{event_type} contract violations: {failures}")
    return {"event_type": event_type, **row}
