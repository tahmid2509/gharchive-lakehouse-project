# Phase 2 — incremental Silver Lakeflow pipeline

This folder builds the nine Silver tables defined in `05_silver_data_profiling.ipynb` from `github_lakehouse.bronze.github_events_raw`. All project changes are contained in `Notebooks/Silver_layer`. The existing profiling notebook is retained as evidence.

## Files and execution order

| File | Purpose |
|---|---|
| `06_silver_preflight.py` | Job notebook: verify Bronze schema/scope and inspect every duplicate-ID group for payload/type conflicts. |
| `07_silver_pipeline.py` | The only Lakeflow pipeline library: source router, nine typed event branches, quarantine, and keyed Silver outputs. |
| `08_silver_validation.py` | Job notebook: schema and quality checks, quarantine verification, exact key reconciliation and winner-lineage verification. |
| `09_pilot_performance.py` | Run manually after either pilot: query the selected pipeline update, plans, optional Job timings, and current table storage. |
| `silver_contract.py` | Explicit narrow JSON schemas, field mappings, casts, normalization, and shared quality rules. |
| `silver_validation.py` | Batch audit functions. |
| `silver_pilot_audit.py` | Source inventory, semantic measurements, quarantine evidence, and comparison of two validation reports. |
| `databricks.yml` | Complete deployable pipeline and three-task Job configuration. |
| `tests/test_silver.py` | Spark fixture tests and deployment graph checks. |
| `tests/test_pilot_audit.py` | Focused tests for the additional evidence queries. |
| `PILOT_RUNBOOK.md` | Workspace deployment, first run, second run, performance collection, and interpretation. |

The numbered `.py` files are Databricks source notebooks. Importing/pulling them into a Databricks Git folder displays them as notebooks. Helper `.py` files are ordinary Python modules.

```text
Job: preflight → Silver pipeline → validate_silver

Pipeline: Bronze Delta stream
          → private retained_event_router (partitioned by event_type)
          → 9 narrow candidate views
             ├─ valid → 9 keyed Silver streaming tables
             └─ invalid → github_events_quarantine
```

## Output tables and cleaning policy

The production target is `github_lakehouse.silver`:

`push_events`, `pull_request_events`, `issue_events`, `issue_comment_events`, `pull_request_review_events`, `pull_request_review_comment_events`, `watch_events`, `fork_events`, `release_events`, plus operational `github_events_quarantine`.

Every event table includes `event_id`, `event_type`, `event_timestamp`, `event_date`, `actor_id`, `actor_login`, `repo_id`, `repo_name`, `_source_file`, `_source_date`, and `_ingested_at`. Additional columns, exact nested source paths, and SQL types are listed directly in `silver_contract.EVENTS`.

| Profiling finding / contract | Implemented behavior |
|---|---|
| Nine retained, six excluded types | Filter the router to retained types; excluded valid events remain in Bronze. NULL/blank event types reach quarantine. |
| Nested source objects | Parse a different narrow `StructType` per event, then project flat named fields. Preserve one row per event. |
| `payload.commits[]` and other unused arrays | Omitted as required by the locked plan. Push volume comes from `payload.size`/`distinct_size`; exploding commits would change the table grain. |
| Intended BIGINT/TIMESTAMP/BOOLEAN types | Parse scalar leaves as strings, then `try_cast`. A non-NULL value that fails casting is quarantined, including optional timestamps. |
| Common/lineage and event-specific required values | Quarantine missing or blank required strings and NULL required typed fields. Never blanket-drop rows with nullable fields. |
| PR/issue lifecycle NULLs | Preserve NULL closure/merge timestamps; do not impute dates. |
| `pr_merged=true` without merge time; impossible timestamp order | Quarantine. Negative Push/PR counts also quarantine. |
| `pr_merged=false` with merge time | Preserve with a warning, following the handoff's informational inverse rule. |
| Two incomplete IssueComment rows | Quarantine with `MISSING_ISSUE_COMMENT_PAYLOAD`. Missing issue data never becomes an invented issue target. |
| IssueComment target | `pull_request` when `payload.issue.pull_request.url` is non-NULL; otherwise `issue` when issue ID exists. |
| 611 Push rows with distinct count > total | Preserve both source values and record a warning. |
| 278 releases with missing published timestamp | Preserve NULL in Silver and report a warning. G5 eligibility is evaluated later in Gold. |
| One blank release tag | Empty/whitespace-only tag becomes NULL; other tags retain their source spelling. |
| Action/review state values | Preserve source values, including `pending` and `dismissed`; warn for previously unobserved values. |
| Source-date mismatch | Warn and preserve. Event date is always derived from the UTC event timestamp. |
| Duplicate event IDs | Keyed SCD Type 1 AUTO CDC, with maximum `struct(_ingested_at, _source_file)` winning deterministically. |

Quarantine preserves each invalid Bronze occurrence and raw JSON. `failure_reason` is the first failure; `failure_reasons ARRAY<STRING>` records every violated rule. Quarantine is append-only and checkpointed; repeated identical invalid source occurrences remain countable. Source duplicates and Job retries are different: checkpoints prevent retries from appending the same occurrence again.

## Required Databricks settings and permissions

- Unity Catalog workspace with the `github_lakehouse` catalog and existing Bronze Delta table.
- Serverless Lakeflow pipelines and serverless Jobs available to the run identity. The bundle uses `CURRENT`, triggered mode (`continuous: false`), Advanced edition, Photon, and production execution mode (`development: false`). Use the current Databricks `pyspark.pipelines` API.
- Run identity: the deploying user by default. It needs `USE CATALOG`, `USE SCHEMA` and `SELECT` on Bronze, and `USE SCHEMA`/`CREATE TABLE` on the target schema. To let deployment create a missing target schema, grant `CREATE SCHEMA` on the catalog; otherwise create the selected schema (`silver_dev`, `silver_day_test`, or `silver`) first and grant the appropriate privileges. Managed catalog storage must already be configured.
- No S3 keys, raw-file access, external Python packages, manual checkpoints, SQL warehouse ID, or cluster ID are required by this Silver bundle. It reads the existing Bronze table through Unity Catalog.
- Stop Bronze ingestion while this Job runs. Preflight records a Delta version; validation fails if Bronze advances during the run. This prevents a misleading comparison between different snapshots. Appends between completed Job runs are supported.
- The nine outputs are managed by one pipeline. Deploy only one production instance against `github_lakehouse.silver`; other users/deployments must use distinct target schemas.

## Deploy and run

Use the current Databricks CLI with bundle support. From a local checkout:

```powershell
cd Notebooks/Silver_layer
databricks auth login --host https://YOUR-WORKSPACE-HOST --profile gharchive
databricks bundle validate -t dev -p gharchive
databricks bundle deploy -t dev -p gharchive
databricks bundle run silver_job -t dev -p gharchive
```

`dev` reads only `2024-02-01-0.json.gz`, requires one source file, and publishes to `github_lakehouse.silver_dev`. It is a separate pipeline with separate checkpoints from production. The pipeline task creates all nine tables automatically, even when a particular event has zero valid rows in a small test scope.

The same code also includes a `day_test` target. It uses all 24 hours of 2024-02-01 and publishes to `github_lakehouse.silver_day_test`. Both `dev` and `day_test` enable the additional pilot audit. Select the scope through deployment configuration; there is no copied one-day pipeline.

For the one-day pilot:

```powershell
databricks bundle validate -t day_test -p gharchive
databricks bundle deploy -t day_test -p gharchive
databricks bundle run silver_job -t day_test -p gharchive
```

Each target has its own deployment state, pipeline ID, checkpoints, and schema. Keep the existing `dev` scope unchanged when redeploying it. Expanding the source filter on its already-checkpointed pipeline does not reliably replay older filtered rows. Use `day_test` for the broader initial read.

After the first successful pilot run, save its output, then run the same Job again with Bronze unchanged and `full_refresh: false`. Follow [PILOT_RUNBOOK.md](PILOT_RUNBOOK.md) to compare the reports and collect performance evidence. Stop after this pilot. The existing `prod` target remains available for a separately authorized full-week run; it is not deployed or run by selecting `day_test`.

The bundle is just a deployment definition; no GitHub Actions/CI platform is needed. You can also deploy the nested `databricks.yml` using the workspace bundle editor. The bundle commands deploy only this Silver folder.

## Parameters

These are **deployment variables**, shared by the pipeline configuration and Job notebook task parameters. Set them in `databricks.yml` or with `BUNDLE_VAR_<name>` before **both validate and deploy**. The Job then uses the deployed values. Pipeline tasks do not inherit notebook widget overrides; changing only a notebook task parameter would audit a different scope than the pipeline.

| Parameter | Production default | Meaning |
|---|---|---|
| `catalog` | `github_lakehouse` | Output catalog. |
| `silver_schema` | `silver` | Target schema; use an isolated schema for pilots. |
| `bronze_table` | `github_lakehouse.bronze.github_events_raw` | Fully qualified source table. |
| `source_start_date` | empty | Optional inclusive `_source_date` lower bound. |
| `source_end_date` | empty | Optional inclusive upper bound. |
| `source_file_name` | empty | Optional exact Bronze `_source_file_name`, for one-hour testing. |
| `max_bytes_per_trigger` | `1g` | Soft source micro-batch byte limit; tune after observing the pilot. |
| `expected_source_files` | `0` | 0 reports coverage; a positive value enforces it. Use 168 for an initial full-week gate. |
| `max_quarantine_rows` | `-1` | -1 reports all quarantine; nonnegative value fails validation above that count. Use 2 only after confirming the historical baseline. |
| `pilot_audit` | `false` | Extra evidence queries; enabled in `dev` and `day_test`. Measures semantic anomalies without changing the cleaning contract. |

Automatically wired settings:

- Pipeline `silver.source_path` and notebook `source_path`: `${workspace.file_path}`, so shared modules import from the deployed Silver folder.
- Pipeline configuration keys: `silver.bronze_table`, `silver.source_start_date`, `silver.source_end_date`, `silver.source_file_name`, `silver.max_bytes_per_trigger`.
- Validation `bronze_version`: `{{tasks.preflight.values.bronze_version}}`; this comes from preflight, never a manually guessed version.
- UTC timezone is set in every notebook.
- Job concurrency: 1, queue enabled, timeout: 14,400 seconds. Preflight and validation have zero retries; the pipeline task has one retry after 60 seconds. Dependencies require upstream success.
- Schedule: absent (run on demand after Bronze ingestion). Add scheduling only when Bronze ingestion has a known schedule. No notification recipient is invented.

For manual UI setup, create a pipeline with this folder as its root, add **only `07_silver_pipeline`** as a library, set the catalog/schema and the six configuration keys above, and choose serverless/triggered/Advanced/CURRENT. Create the three Job tasks with the notebook paths and `base_parameters` shown in `databricks.yml`, referencing that pipeline's ID. A pipeline task manages its own compute; the two notebook tasks use serverless Jobs. Pull the complete folder, including all helper modules. Notebook 09 is an optional manual evidence collector, not a pipeline library or fourth Job task.

## Correctness and performance

Preflight checks all duplicate groups, not the earlier sample of 20. It shuffles only event IDs/types/counts first, then compares raw JSON within the duplicate groups. Conflicting payloads or event types fail the Job before output processing. Required-null failures route to quarantine; warning expectations keep rows. The output deduplication key is the GitHub event ID, never PR ID or issue ID, because multiple lifecycle events for one PR/issue must remain separate.

The private router makes the Bronze payload-routing read shared and partitions it by event type. Each branch applies its event filter before one narrow JSON parse. Candidate views can be evaluated separately by clean/quarantine flows: this is deliberately not claimed to be a physical single parse shared between both consumers. Inspect the pilot DAG and file-read metrics before adding another materialized copy. Payload extraction has no joins, Python UDFs, `explode`, sorts, or arbitrary repartition calls. Only keyed upserts and explicit batch audits require key aggregation/distribution.

AUTO CDC provides convergence on one row per event ID without a custom unbounded streaming dedup state or an event-time watermark that could discard historical/late data. It still has storage and upsert cost; inspect that cost on the pilot. Identical key/sequence rows must represent identical projected content. The later duplicate copy can update the chosen lineage. Gold should read these outputs with batch/materialized-view or change-aware semantics, not assume append-only Delta files.

Validation checks schemas, required values, hard rules, warning counts, event-type purity, exact retained-key coverage, duplicate removal, and the maximum-sequence winner lineage. It verifies quarantine against source occurrences and recomputes its rules on the small quarantined set. Its source-key audit reads all selected Bronze history; that batch verification cost is intentional and separate from the incremental transformation. It does not parse nine full copies of Bronze to calculate expected counts.

For each event type:

```text
retained Bronze occurrences = valid Bronze occurrences + quarantined retained occurrences
valid Bronze occurrences = Silver rows + valid duplicate copies removed
```

The full-week profiling baseline is 38,555,222 total Bronze rows, 33,623,876 retained rows, and 85 extra duplicate rows across 78 IDs. The final clean total is computed from actual quarantine/duplicate overlap, rather than assuming 33,623,789 in advance. G5's incomplete release windows remain eligible Silver rows.

## Tests and remaining runtime verification

Run `python -m pytest Notebooks/Silver_layer/tests -q` with PySpark 4.0.1, pytest, PyYAML, and a supported Java installation. Tests cover all nine schemas, source paths, UTC derivation, both comment targets, malformed JSON, missing required values, optional cast failures, lifecycle failures, preserved warnings, duplicate conflicts, quarantine overlap, and winner-lineage reconciliation.

Local Spark validates transformations and audits. AUTO CDC, pipeline checkpoints, Unity Catalog privileges, serverless entitlement, and bundle deployment require the target Databricks workspace. After deployment, run the one-hour Job, repeat it without new input, then use the one-day/full-week progression. Check that warning metrics are visible and only contract failures enter quarantine. A failed expectation rolls back its flow, not every table in the pipeline; downstream Gold must depend on the successful validation task.

## Product references

- [Private tables and mixed pipeline datasets](https://docs.databricks.com/aws/en/ldp/transform)
- [AUTO CDC requirements and sequencing](https://docs.databricks.com/aws/en/ldp/cdc)
- [Quarantine and expectation behavior](https://docs.databricks.com/aws/en/ldp/expectation-patterns)
- [Bundle pipeline and serverless Job examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples)
