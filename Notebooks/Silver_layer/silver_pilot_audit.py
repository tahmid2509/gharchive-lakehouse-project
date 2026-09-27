"""Read-only pilot evidence. No new acceptance rules or pipeline transformations.

Semantic counts use final Silver events (one per event_id), while quarantine
counts use Bronze occurrences. Counts cover the full scope; examples are bounded.
"""
from datetime import date, timedelta

from pyspark.sql import functions as F

from silver_contract import EVENTS, EXCLUDED, LINEAGE


def source_inventory(source, start="", end="", file_name=""):
    types = [r.asDict() for r in source.select(
        F.get_json_object("raw_json", "$.type").alias("event_type"))
        .groupBy("event_type").count().collect()]
    # Both pilots and the historical week fit easily inside this collection bound.
    files = source.groupBy("_source_file", "_source_file_name", "_source_date").count().limit(1001).collect()
    if len(files) > 1000:
        raise AssertionError("Pilot inventory exceeds 1000 file/date groups; check source scope")
    inventory = [r.asDict() for r in files]
    expected = None
    if file_name:
        expected = {file_name}
    elif start and end:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        days = (last - first).days + 1
        if not 1 <= days <= 41:
            raise ValueError("Detailed hourly inventory supports 1 through 41 days")
        expected = {f"{first + timedelta(days=offset)}-{hour}.json.gz"
                    for offset in range(days) for hour in range(24)}
    if expected is not None:
        observed = {r["_source_file_name"] for r in inventory}
        if (observed != expected or len(inventory) != len(expected) or
                len({r["_source_file"] for r in inventory}) != len(expected) or
                any(str(r["_source_date"]) != r["_source_file_name"][:10] for r in inventory)):
            raise AssertionError(f"Expected hourly files do not match selected scope: {inventory}")
    totals = dict(total_bronze_rows=0, retained_bronze_rows=0, excluded_bronze_rows=0,
                  unroutable_bronze_rows=0, unexpected_type_rows=0)
    for row in types:
        kind, count = row["event_type"], row["count"]
        totals["total_bronze_rows"] += count
        bucket = ("retained_bronze_rows" if kind in EVENTS else "excluded_bronze_rows" if kind in EXCLUDED
                  else "unroutable_bronze_rows" if kind is None or not kind.strip() else "unexpected_type_rows")
        totals[bucket] += count
    # Unknown valid types stay excluded by the existing contract, but are visible
    # separately here so the 9+6 accounting cannot hide a new source type.
    return {**totals, "by_event_type": types, "files": inventory,
            "expected_source_files": len(expected) if expected is not None else None,
            "source_files": len({r["_source_file"] for r in inventory})}


def semantic_checks(event_type, columns):
    """FAIL conditions used only for measurement, never as expectations."""
    checks = {}
    for name in ["actor_id", "repo_id", "pr_id", "pr_number", "issue_id", "issue_number",
                 "comment_id", "review_id", "release_id"]:
        if name in columns:
            checks[f"{name}_zero"] = f"{name} = 0"
            checks[f"{name}_negative"] = f"{name} < 0"
    if event_type in ("PullRequestEvent", "IssuesEvent"):
        prefix = "pr" if event_type == "PullRequestEvent" else "issue"
        checks[f"{prefix}_closed_without_closed_at"] = f"{prefix}_action = 'closed' AND {prefix}_closed_at IS NULL"
        checks[f"{prefix}_created_after_event"] = f"{prefix}_created_at > event_timestamp"
    if event_type == "ReleaseEvent":
        checks["release_published_after_event"] = "release_published_at > event_timestamp"
        checks["release_missing_published_at"] = "release_published_at IS NULL"
    return checks


def semantic_profile(df, event_type):
    checks = semantic_checks(event_type, df.columns)
    aggregates = {name: F.count(F.when(F.expr(sql), 1)) for name, sql in checks.items()}
    prefix = {"PullRequestEvent": "pr", "IssuesEvent": "issue"}.get(event_type)
    if prefix:
        for action in ("opened", "reopened", "closed"):
            condition = F.col(f"{prefix}_action") == action
            aggregates[f"{action}_rows"] = F.count(F.when(condition, 1))
            aggregates[f"{action}_closed_at_null"] = F.count(F.when(condition & F.col(f"{prefix}_closed_at").isNull(), 1))
            aggregates[f"{action}_closed_at_present"] = F.count(F.when(condition & F.col(f"{prefix}_closed_at").isNotNull(), 1))
    counts = df.agg(*[expr.alias(name) for name, expr in aggregates.items()]).first().asDict()
    # Select only published fields; no raw payloads or unnecessary nested content.
    examples = {name: [r.asDict() for r in df.filter(sql).limit(3).collect()]
                for name, sql in checks.items() if counts[name]}
    findings = {}
    for name, sql in checks.items():
        if name == "release_missing_published_at":
            recommendation = "INFORMATIONAL ONLY"
            interpretation = "Known valid source behavior; these releases cannot anchor the Gold release window."
        elif counts[name]:
            recommendation = "WARNING"
            interpretation = "Suspicious source value; review the examples and source semantics before considering a hard rule."
        else:
            recommendation = "NO RULE"
            interpretation = "No occurrences in this scope; this is not evidence for introducing a hard rule."
        findings[name] = {"condition": sql, "count": counts[name], "examples": examples.get(name, []),
                          "provisional_recommendation": recommendation, "interpretation": interpretation}
    return {"population": "final Silver events; quarantined occurrences excluded",
            "checks": findings,
            "closed_at_by_action": ({action: {
                "rows": counts[f"{action}_rows"], "null": counts[f"{action}_closed_at_null"],
                "present": counts[f"{action}_closed_at_present"]}
                for action in ("opened", "reopened", "closed")} if prefix else {}),
            "closed_at_profile_recommendation": "INFORMATIONAL ONLY; inspect action-specific counts before changing nullable timestamps",
            "contract_changed": False}


def quarantine_profile(quarantine):
    primary = [r.asDict() for r in quarantine.groupBy("event_type", "failure_reason").count().collect()]
    all_reasons = quarantine.select("event_type", F.explode("failure_reasons").alias("reason"))
    reasons = [r.asDict() for r in all_reasons.groupBy("event_type", "reason").count().collect()]
    examples = []
    for row in reasons:
        bad = quarantine.filter(F.col("event_type").eqNullSafe(F.lit(row["event_type"])) &
                                F.array_contains("failure_reasons", row["reason"]))
        examples.append({**row, "examples": [r.asDict() for r in bad.select(
            "event_id", *LINEAGE, "failure_reason", "failure_reasons",
            F.substring("raw_json", 1, 4000).alias("raw_json_excerpt"),
            (F.length("raw_json") > 4000).alias("raw_json_excerpt_truncated"))
            .limit(3).collect()]})
    return {"rows": sum(r["count"] for r in primary), "by_primary_reason": primary,
            "by_all_reasons": reasons, "examples_by_reason": examples,
            "reason_counts_overlap": True,
            "classification": "Recomputed with locked contract by check_quarantine before this report"}


def pilot_reconciliation(inventory, quarantine_report, reconciliation):
    q_counts = {}
    for row in quarantine_report["by_primary_reason"]:
        kind = row["event_type"]
        q_counts[kind] = q_counts.get(kind, 0) + row["count"]
    source_counts = {r["event_type"]: r["count"] for r in inventory["by_event_type"]}
    rec = {r["event_type"]: r for r in reconciliation}
    rows = []
    for kind, (table, _) in EVENTS.items():
        r = rec.get(kind, {"valid_bronze_rows": 0, "silver_rows": 0, "duplicates_removed": 0})
        candidate, invalid = source_counts.get(kind, 0), q_counts.get(kind, 0)
        if candidate != r["valid_bronze_rows"] + invalid:
            raise AssertionError(f"Candidate/quarantine reconciliation failed for {kind}")
        if r["valid_bronze_rows"] != r["silver_rows"] + r["duplicates_removed"]:
            raise AssertionError(f"Valid/duplicate reconciliation failed for {kind}")
        rows.append({"event_type": kind, "table": table, "bronze_candidate_rows": candidate,
                     "quarantine_rows": invalid, **r, "event_id_unique": True,
                     "winner_lineage_verified": True})
    totals = {"retained_bronze_occurrences": inventory["retained_bronze_rows"],
              "valid_candidate_occurrences": sum(r["valid_bronze_rows"] for r in rows),
              "quarantined_retained_occurrences": sum(r["quarantine_rows"] for r in rows),
              "final_silver_rows": sum(r["silver_rows"] for r in rows),
              "valid_duplicate_copies_removed": sum(r["duplicates_removed"] for r in rows)}
    return {"tables": rows, "totals": totals,
            "retained_equals_valid_plus_quarantine": True,
            "valid_equals_silver_plus_duplicates": True}


def compare_pilot_reports(first, second):
    """Compare two successful validation JSON objects from an unchanged source.

    Both runs independently prove key uniqueness, exact quarantine occurrence
    coverage, and winner lineage. This compares their stable numerical evidence.
    Timings and representative examples may legitimately differ between runs.
    """
    if any(r.get("status") != "passed" or not r.get("pilot_report") for r in (first, second)):
        raise ValueError("Supply two complete successful validation reports with pilot_audit=true")
    if first["bronze_version"] != second["bronze_version"]:
        raise AssertionError("Bronze changed between runs; this is not an unchanged-source idempotency test")
    if first["pilot_report"]["configuration"] != second["pilot_report"]["configuration"]:
        raise AssertionError("Source scope or target changed between runs")

    def counts(report):
        quality = {row["event_type"]: {k: v for k, v in row.items()
                   if isinstance(v, (int, float)) or k in ("schema", "column_order")}
                   for row in report["tables"]}
        semantics = {row["event_type"]: {
            "checks": {k: v["count"] for k, v in row["semantic_profile"]["checks"].items()},
            "actions": row["semantic_profile"]["closed_at_by_action"]} for row in report["tables"]}
        q = report["pilot_report"]["quarantine"]
        reasons = {(row["event_type"], row["reason"]): row["count"] for row in q["by_all_reasons"]}
        return {"quality": quality, "semantics": semantics, "quarantine_rows": report["quarantine_rows"],
                "quarantine_reasons": reasons,
                "reconciliation": report["pilot_report"]["reconciliation"]}

    if counts(first) != counts(second):
        raise AssertionError("Validation counts/schema/quality changed between runs; inspect both reports")
    return {"status": "passed", "bronze_version": first["bronze_version"],
            "counts_and_quality_unchanged": True, "quarantine_occurrences_unchanged": True,
            "key_uniqueness_and_winner_lineage": "Independently verified by both successful validations",
            "checkpoint_execution": "Review the two pipeline updates with 09_pilot_performance"}
