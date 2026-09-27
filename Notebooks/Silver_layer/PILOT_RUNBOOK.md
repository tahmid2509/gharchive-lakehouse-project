# Running and explaining the Silver pilot

Use the existing Git folder and the same three Job notebooks. The target in `databricks.yml` chooses the source scope and output schema. No copying or combining notebook code is necessary.

## 1. Pull and deploy

After merging this pull request on GitHub:

1. In Databricks Workspace, open the project's Git folder, select `main`, and **Pull**.
2. Open `Notebooks/Silver_layer/databricks.yml`.
3. Open its **Deployments** pane and select **day_test**.
4. Click **Deploy**, review the target and resources in the confirmation dialog, then complete deployment.
5. Open the deployed Job **gharchive-silver-day_test** from **Bundle resources**. Run the Job, so preflight and validation run with the pipeline.

The workspace editor supports deploying the bundle and running its resources directly; no local CLI installation or MCP connection is required. [Databricks workspace deployment instructions](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-deploy)

| Setting | `dev` (one hour) | `day_test` (one day) |
|---|---|---|
| Source | `github_lakehouse.bronze.github_events_raw` | Same |
| Start / end source date | Empty / empty | `2024-02-01` / `2024-02-01` |
| Filename filter | `2024-02-01-0.json.gz` | Empty |
| Expected source files | 1 | 24 |
| Target schema | `github_lakehouse.silver_dev` | `github_lakehouse.silver_day_test` |
| Job and pipeline name | `gharchive-silver-dev` | `gharchive-silver-day_test` |
| Detailed pilot audit | Enabled | Enabled |
| Full refresh | False | False |

Both targets use `06_silver_preflight`, `07_silver_pipeline`, and `08_silver_validation`. Redeploying `dev` updates its audit code and preserves its existing checkpointed scope. Choosing `day_test` creates a separate pipeline that can read all 24 historical files. Merely widening the `dev` filter can miss previously filtered historical input.

Keep Bronze ingestion stopped during both pilot runs. Preflight saves its Delta version; validation reads that version and checks that Bronze has not advanced. No source version is guessed and no production data is deleted. The Job remains on demand, with concurrency 1, a four-hour timeout, and the existing single pipeline retry. Notebook 07 is still the only pipeline library.

## 2. Run once and save the reports

Click **Run now** on the deployed Job. After all three tasks succeed, open the run and click each notebook task to view its output.

- Save the final JSON object from **preflight** as your first preflight report. It contains the Bronze version, source rows/files, duplicate counts, payload/type conflict checks, and up to 100 duplicate winner examples.
- Save the final JSON object from **validate_silver** as your first validation report. It begins with `"status": "passed"` and includes `tables`, `reconciliation`, and `pilot_report`.
- Record the Job run ID, pipeline ID, and pipeline update ID locally for notebook 09. No workspace URL or credentials need to be shared.

Validation also prints individual table reports before the final combined object. Copy the final combined object for rerun comparison. All numerical counts cover the full selected scope; only representative examples are limited.

| Question | Where the answer appears |
|---|---|
| Exact files and source dates | `pilot_report.source_inventory.files` |
| Total / retained / excluded / unroutable / unexpected types | `pilot_report.source_inventory` |
| Candidate, valid, Silver, quarantine, valid duplicate counts per table | `pilot_report.reconciliation.tables` |
| Both reconciliation equations | `pilot_report.reconciliation.totals` and the two verified booleans |
| Exact schema and column order | `tables[].schema` and `tables[].column_order` |
| Required and hard-rule failures in final Silver | `tables[]` fields named `required_*` and the named hard rules; any failure stops validation |
| Failed casts and other rejected source values | `pilot_report.quarantine.by_all_reasons`, including `INVALID_CAST` |
| Quarantine primary categories and samples | `pilot_report.quarantine.by_primary_reason` and `examples_by_reason` |
| Existing warnings | `tables[]` fields beginning `warn_` |
| PR / Issue / Release / numeric-ID semantic checks | `tables[].semantic_profile.checks` |
| Closure timestamps for opened / reopened / closed actions | `tables[].semantic_profile.closed_at_by_action` |
| Unique event IDs and deterministic winner lineage | Passed reconciliation; each table entry records these checks |
| Actual duration of notebook audit code | `audit_elapsed_seconds`; excludes compute startup |

Quarantine counts are **source occurrences**, including repeated invalid source records. The reason array may contain multiple reasons for one row, so all-reason counts must not be summed as a quarantine row total. Cast failures are distinguishable from legitimate source NULLs because the existing contract checks raw scalar presence before casting. Full raw JSON remains in quarantine; printed excerpts are capped at 4,000 characters and marked when truncated.

Semantic counts are **final Silver events**, after quarantine and deduplication. A failed timestamp cast in quarantine therefore does not masquerade as a legitimate NULL in this profile. Quarantined cases remain available in the separate failure evidence. Zero/negative identifiers and suspicious action/time combinations are measured, with up to three examples each. The provisional recommendations explain what to investigate; they do not create new expectations or remove rows.

`release_published_at IS NULL` remains legitimate and informational for Silver. Decisions about release-window eligibility belong in Gold. Review the actual examples before deciding whether any additional semantic check deserves a warning or hard rule.

## 3. Rerun the same Job

After saving the first reports, run the same deployed Job again without changing Bronze, the scope, or the pipeline configuration. Leave **full refresh disabled**. Save the second preflight and validation reports.

To compare the validation results automatically, create a Python notebook in this same Silver folder (or use a scratch notebook and add the folder to `sys.path`). Paste the two complete final JSON objects into this cell:

```python
import json
from silver_pilot_audit import compare_pilot_reports

first = json.loads(r'''PASTE FIRST VALIDATION JSON HERE''')
second = json.loads(r'''PASTE SECOND VALIDATION JSON HERE''')
print(json.dumps(compare_pilot_reports(first, second), indent=2))
```

The comparison rejects different Bronze versions or scopes and changed table counts, schemas, quality counts, semantic counts, quarantine counts/reasons, or reconciliation results. Both successful validations separately check event-ID uniqueness, winner lineage, and exact quarantine occurrence coverage. This is evidence for the requested rerun properties; it is not a byte-for-byte checksum of every table value. Notebook 09 supplies separate evidence about checkpoint execution and work performed on each update.

If any task fails, save its exception and stop before expanding the scope. Duplicate payload/type conflicts are already preflight failures; inspect them before changing the key or sequencing contract.

## 4. Collect pilot performance

Open **09_pilot_performance** in this folder and attach available notebook compute. Run the first cell once to create its widgets, fill them, and select **Run all**:

| Widget | Value |
|---|---|
| `pipeline_id` | ID of the deployed pilot pipeline |
| `update_id` | ID of the pipeline update belonging to the Job run being measured |
| `job_run_id` | Optional numeric ID of the completed Job run |
| `catalog` | `github_lakehouse` |
| `silver_schema` | `silver_day_test`, or `silver_dev` when examining the one-hour run |

Find the Job run ID on the completed run's details page. From that run's pipeline task, open its pipeline update and copy the pipeline ID and update ID. Use the update linked to the selected Job run, rather than assuming the latest update is the right one. Run notebook 09 for each of the two updates and save/export its displayed tables and printed JSON.

The notebook reads the selected update's event log, flow output metrics, query plans, stream/operation details, and current storage metadata for the ten public tables. Optional Job metadata uses the installed Databricks SDK and your own notebook identity to read Job/task durations. If SDK access is unavailable, copy those durations from the Job run details. API durations are milliseconds. [Databricks SDK authentication](https://docs.databricks.com/aws/en/dev-tools/sdk-python)

Event-log access depends on ownership and workspace permissions. If it is denied, preserve the error and use the pipeline UI's exposed metrics; no additional privileges are created by this code. [Event-log function](https://docs.databricks.com/aws/en/sql/language-manual/functions/event_log)

For the performance discussion, keep the following distinctions:

- Current table `sizeInBytes` is storage at collection time, not bytes written by the selected update. Collect immediately after each run if you want before/after storage evidence.
- Router output rows are in its flow metrics when exposed. Its expected first-run population is retained plus unroutable Bronze occurrences. Private router storage is not queried through an invented public table name.
- Source bytes, target bytes, shuffle, and AUTO CDC internals depend on available telemetry. Missing values are unavailable, not zero. Keep metrics attached to their flow/update/timestamp; do not sum successive cumulative snapshots.
- A query plan can show filters and parsing expressions. It cannot by itself prove saved read bytes, reduced JSON parsing CPU, or lower cost. Evaluate actual scan/shuffle/runtime evidence before recommending a change.
- Audits intentionally scan the selected history. Report their time separately from incremental pipeline transformation time.

The event log exposes flow metrics, update progress, and flow definitions including explained plans. [Databricks event-log schema](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)

## 5. Explain the implementation in an interview

“Bronze keeps the original JSON and ingestion metadata. One streaming router selects the nine useful event types. Each event branch parses only its required fields, casts types, and separates valid records from contract failures. AUTO CDC keeps one event per ID using deterministic ingestion lineage. A Job checks the source before processing and reconciles the published data afterward. Changing from one hour to one day is a deployment configuration change. The audit queries measure extra anomalies before we decide whether to enforce new rules.”

There are no new public audit tables, UDFs, additional services, or alternative cleaning engines. The supplied handoff, architecture markdown, schema PDF, Bronze schemas, and profiling notebook determine the existing nine-table contract. The older Gold-plan markdown's earlier Silver proposal is superseded by the handoff's nine-table decision; the six Gold products remain the downstream requirements. Optional lifecycle timestamps, source-valid Push anomalies, and release eligibility follow those documents.

Stop after the pilot and its rerun. The actual A–K results report can be completed from these outputs: configuration; execution; counts; reconciliation; quality; semantic findings; deduplication; idempotency; performance; discovered problems; and readiness for a separately approved full-week test. No one-day counts or performance conclusions are assumed in advance.
