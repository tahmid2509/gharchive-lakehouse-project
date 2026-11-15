# Gold layer contract

This file is the single written source of truth for the six Gold tables of the GitHub Activity Lakehouse.

- It fixes what each table means, which columns it has, which rows a metric may use and which checks every run must pass.
- **A rule or a column changes in this file first, and in code second.**
- It contains no code. The notebooks in `Notebooks/Gold_layer/` implement it.

| | |
|---|---|
| Data window | 2024-02-01 00:00 UTC up to, but not including, 2024-02-08 00:00 UTC (168 hourly GH Archive files) |
| Based on | Completion Manual (Part IV and V), Silver Schema Reference, revised NULL and Quarantine Policy, the Silver notebooks on `main`, and the run of `00_gold_data_profiling` on 14 November 2026 |
| Status | Rules and decisions approved on 14 November 2026. Written so far: the Gold setup (`08`), the source-hour manifest (`07`) and the first Gold table, G2 (`02`). The manifest ran on the real data on 15 November 2026: 168 hours, all complete. Its test notebook passed in Databricks on the same day: 12 of 12 sections, 145 checks. G2 ran on the real data on 15 November 2026 (UTC): 194,627 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 135 checks. G2 is the only Gold table built so far |

## Contents

1. [Sources](#1-sources)
2. [Rules every Gold table follows](#2-rules-every-gold-table-follows)
3. [Decisions](#3-decisions)
4. [Names](#4-names)
5. [Conventions](#5-conventions)
6. [How a Gold table is refreshed](#6-how-a-gold-table-is-refreshed)
7. [The Gold audit table](#7-the-gold-audit-table)
8. [The source-hour manifest](#8-the-source-hour-manifest)
9. [G1 `repository_delivery_metrics`](#9-g1-repository_delivery_metrics)
10. [G2 `repository_issue_flow`](#10-g2-repository_issue_flow)
11. [G3 `repository_contributor_concentration`](#11-g3-repository_contributor_concentration)
12. [G4 `repository_collaboration_metrics`](#12-g4-repository_collaboration_metrics)
13. [G5 `release_activity_shift`](#13-g5-release_activity_shift)
14. [G6 `repository_activity_profile`](#14-g6-repository_activity_profile)
15. [The final Gold audit](#15-the-final-gold-audit)
16. [Tests](#16-tests)
17. [Code style](#17-code-style)
18. [Measured facts](#18-measured-facts)
19. [Change log](#19-change-log)

## 1. Sources

Gold reads the nine Silver tables in `github_lakehouse.silver`. The row counts are those of the all-dates Job run and were confirmed by the profiling run.

| Silver table | Rows | Read by |
|---|---|---|
| `push_events` | 26,536,967 | G3, G5, G6 |
| `pull_request_events` | 2,173,958 | G1, G3, G5, G6 |
| `issue_events` | 548,956 | G2, G3, G5, G6 |
| `issue_comment_events` | 1,357,324 | G3, G4, G6 |
| `pull_request_review_events` | 717,277 | G3, G4, G6 |
| `pull_request_review_comment_events` | 422,917 | G3, G4, G6 |
| `watch_events` | 1,380,559 | G5, G6 |
| `fork_events` | 326,417 | G5, G6 |
| `release_events` | 159,416 | G5 (anchors), G6 |
| **Total** | **33,623,791** | |

Every Silver table has the common columns `event_id`, `event_type`, `event_timestamp`, `event_date`, `actor_id`, `actor_login`, `repo_id`, `repo_name`, the lineage columns `_source_file`, `_source_date`, `_ingested_at`, and `quarantine_warning`.

## 2. Rules every Gold table follows

| # | Rule | In practice |
|---|---|---|
| 1 | Gold reads only Silver. | Never Bronze and never another Gold table. The one addition is G5, which also reads the source-hour manifest. |
| 2 | Every table has a declared grain. | The grain is the meaning of one row. Each run checks that no key appears twice. |
| 3 | IDs are keys, names are labels. | Group and join on `repo_id` and `actor_id`. `repo_name` and `actor_login` are for display only. |
| 4 | Event counts and entity counts are different things. | Four events about one pull request are four events and one pull request. Column names say which is counted: `..._events` and `..._actions` count events, `..._rows` counts the rows a number was calculated from, `distinct_...` and `unique_...` count things or people. |
| 5 | Every average and percentage shows its base. | Next to each one there is a column with the number of rows it was calculated from: its eligible rows. |
| 6 | NULL is not zero. | NULL means "we could not measure this". Zero means "we measured it and it was zero". A missing value is never replaced by 0 or false. |
| 7 | Percentages are safe. | When the base is 0, the percentage is NULL, not 0. |
| 8 | Event time for analysis, source date for scope. | Days come from `event_date` (UTC). `_source_date` is used only to find which days a run has to rebuild. |
| 9 | No invented dates. | A row without an event time cannot be placed on a day. It is left out of the daily tables and counted in the audit row. |
| 10 | Stack raw events, do not join them. | To combine event types, the tables are placed under each other (a union) with the same columns. Raw events from two tables are never joined to each other, because that multiplies rows. G5 is the one place with a real join, by repository and time window. |
| 11 | Gold is rebuilt, not appended to. | A new or corrected event must change a Gold row that already exists. See section 6. |
| 12 | Honest wording. | Results are "observed in this week". They are not lifetime rates, and a change after a release is not proof that the release caused it. |

**Rules 5, 6 and 7 with numbers.** A repository has 4 closed pull request events on 5 February. For one of them the merged flag is unknown. Then `pr_closed_actions` is 4 and `eligible_closed_rows` is 3. If two of the three were merged, `merged_closed_rows` is 2 and `merge_share_pct` is 66.67. The reader sees that the percentage rests on 3 rows, not 4. On a day with no closed event at all, the share is NULL, because there is nothing to divide by.

## 3. Decisions

The Completion Manual marks its Gold grains, columns and formulas as proposals. These are the choices that were made.

| # | Topic | Decision |
|---|---|---|
| 1 | Grain of G1, G2, G3, G4 and G6 | One row per repository per UTC day. |
| 2 | How Gold is refreshed | A run rebuilds the days it touches and replaces them. A blank date rebuilds everything. G5 is always rebuilt completely. |
| 3 | When results become visible | Build into a staging copy, check it, then publish. A wrong result never replaces a good table. |
| 4 | G1 merge share | The share of closed **events** that were merged. |
| 5 | G1 close duration | Included: average and median hours from creation to close. |
| 6 | Bots in G3 | Every actor is kept in version 1, and the documentation says so. |
| 7 | Concentration index in G3 | The Herfindahl index (HHI) on a 0 to 1 scale is included. |
| 8 | Review states in G4 | Five columns: approved, changes requested, commented, dismissed, other. |
| 9 | Prereleases in G5 | Included, with the column `is_prerelease`. An unknown flag is never treated as false. |
| 10 | Several events for one release in G5 | One anchor per `repo_id` and `release_id`: the earliest event. A release without an ID is left out and counted. |
| 11 | Proof of complete hours for G5 | A manifest built from Bronze. The planned one-time comparison with the line counts of the raw files in S3 is not made, because the S3 connection was removed on purpose after the week was loaded. See section 8. |
| 12 | G6 scope | Core columns first. Optional columns in a second small step. |
| 13 | Labels and rounding | `repo_name` is included as a label: the latest name seen that day. Percentages run from 0 to 100 with 2 decimals. Hours have 2 decimals. |

## 4. Names

| What | Name |
|---|---|
| Gold tables | `github_lakehouse.gold.<table>`. Only the six tables of this file live in this schema. |
| Staging copies | `github_lakehouse.gold_staging.<table>`. Overwritten on every run. Not meant for reading. In a test schema the staging copy is called `<table>_staging`. |
| Gold run audit | `github_lakehouse.audit.gold_pipeline_runs` |
| Source-hour manifest | `github_lakehouse.audit.source_hour_manifest` |
| Notebooks | `Notebooks/Gold_layer/` |
| Test notebooks | `Notebooks/Gold_layer/tests/`, one per Gold notebook |

| Notebook | Purpose |
|---|---|
| `00_gold_data_profiling` | Read-only checks before coding |
| `01_repository_delivery_metrics` | G1 |
| `02_repository_issue_flow` | G2 |
| `03_repository_contributor_concentration` | G3 |
| `04_repository_collaboration_metrics` | G4 |
| `05_release_activity_shift` | G5 |
| `06_repository_activity_profile` | G6 |
| `07_source_hour_manifest` | Builds the source-hour manifest for G5 |
| `08_gold_audit_table` | Creates the staging schema and the Gold audit table |
| `09_gold_final_audit` | Cross-checks the six tables after they are built |

The Gold tables are plain Delta tables without partitions. With seven days and at most a few million rows each, partitions would add work and no benefit.

## 5. Conventions

### Data types

| Kind of column | Type | Rule |
|---|---|---|
| IDs (`repo_id`, `release_id`) | `BIGINT` | As in Silver |
| `activity_date` | `DATE` | The UTC `event_date` of the events |
| Counts | `BIGINT` | 0 when nothing was counted. Never NULL, unless a table section says so |
| Percentages and shares | `DOUBLE` | Rounded to 2 decimals. NULL when the base is 0. A share lies between 0 and 100. The percent change in G5 can be below 0 or above 100 |
| Hours and averages | `DOUBLE` | Rounded to 2 decimals. NULL when there is no eligible row |
| HHI | `DOUBLE` | 0 to 1, rounded to 4 decimals. NULL when there is no attributable event |
| Flags | `BOOLEAN` | |
| Times | `TIMESTAMP` | UTC |

Rounding happens once, at the end. A value is calculated from the unrounded numbers and rounded when it is written. Exact values can always be recalculated from the count columns.

The columns of a table are stored in the order in which its section lists them.

### Hours

A duration in hours is the number of seconds between two times divided by 3,600.

### Median

The median columns (`p50_...`) hold the exact median, so the same input always gives the same value. With an odd number of values it is the middle value. With an even number it is the average of the two middle values: the median of 1, 2, 3 and 10 hours is 2.50 hours.

### The `repo_name` label

`repo_name` is the name on the latest event of that repository on that day, among the events the table reads. If two events share the latest second, the one with the larger `event_id` wins. `event_id` is text and is compared as text. A missing name is skipped. A repository-day whose events all have no name has the label NULL. In G5 the label is the name on the anchor event.

### Technical columns

Every Gold table ends with these two columns.

| Column | Type | Meaning |
|---|---|---|
| `_gold_run_id` | `STRING` | The run that last wrote the row |
| `_gold_updated_at` | `TIMESTAMP` | When that run wrote it |

## 6. How a Gold table is refreshed

Silver uses an insert-only MERGE: a new event is a new row, and old rows never change. A Gold row is a summary, so a new event has to change a row that already exists. Gold is therefore rebuilt for the days a run touches.

### Parameters

Gold notebooks use the same three widgets as Silver.

| Widget | Meaning in Gold |
|---|---|
| `source_date` | A date such as `2024-02-03` rebuilds the Gold days touched by that source date. Blank rebuilds all days. Any other text stops the notebook before it writes. |
| `test_schema` | Blank means production. A name such as `test_gold_g2` makes the notebook read its Silver tables from that schema and write its Gold, staging and audit tables there. G5 also reads its manifest there. The name must start with `test_` and may contain only lower-case letters, digits and underscores. Any other text stops the notebook before it writes. |
| `run_id` | The Job run ID, or a generated `manual-...` value when the notebook is run by hand. It may contain only letters, digits, `-` and `_`. |

The manifest notebook has one more widget, `check_s3`. See section 8.

### A run of a daily table (G1, G2, G3, G4, G6)

1. **Find the affected days.** With a `source_date`, these are the event dates found in the Silver rows of that source date, in any Silver table the notebook reads. With a blank date, they are all days. A row in the scope of the run that has no event date is left out and counted in `rows_excluded` under the key `missing_event_date`.
2. **Build.** Calculate the Gold rows for those days from all Silver rows of those days, not only from the rows of this source date.
3. **Stage.** Write the result to the staging copy.
4. **Check.** Run the quality gates of the table on the staging copy.
5. **Publish.** Replace exactly those days in the Gold table with the staged rows, as one Delta commit. With a `source_date` the statement is `INSERT INTO ... REPLACE WHERE activity_date IN (the affected days)`. With a blank date the whole table is replaced with `INSERT OVERWRITE`.
6. **Audit.** Write one row to `audit.gold_pipeline_runs`.

When there is no affected day, nothing is published and the status is `NO_INPUT`.

What this guarantees:

- Running the same date twice gives the same table.
- A late or corrected event changes the row it belongs to.
- A repository-day that no longer has events disappears, because the whole day is replaced.
- Readers never see a half-written or unchecked table.

### G5

A release needs data from the day before and the day after, so one new day can make older releases eligible. G5 is rebuilt completely on every run, whatever `source_date` says. It is small: 159,416 release events.

### Status

| Status | Meaning |
|---|---|
| `SUCCEEDED` | The gates passed and the result was published. |
| `NO_INPUT` | Silver has no rows in the scope of this run. Gold is unchanged and the task does not fail. |
| `FAILED` | A gate or a write failed. Nothing was published. The audit row is written first, then the notebook raises an error so that the Job task fails. |

A run with a blank date replaces the whole table. It is also the way to remove a day that no longer exists in Silver at all.

## 7. The Gold audit table

`github_lakehouse.audit.gold_pipeline_runs` gets exactly one row per run of a Gold notebook. The manifest notebook writes its row here too, with the manifest as `gold_table`.

| Column | Type | Meaning |
|---|---|---|
| `audit_id` | `STRING` | Unique ID of this audit row |
| `run_id` | `STRING` | Job run ID, or `manual-...` |
| `pipeline_name` | `STRING` | For example `gold_repository_issue_flow` |
| `gold_table` | `STRING` | Full name of the table that was written |
| `processing_scope` | `STRING` | `SINGLE_DATE` or `ALL_DATES` |
| `source_date` | `DATE` | The parameter. NULL for all dates |
| `affected_dates` | `ARRAY<DATE>` | The days this run rebuilt |
| `run_started_at` | `TIMESTAMP` | |
| `run_finished_at` | `TIMESTAMP` | |
| `status` | `STRING` | `SUCCEEDED`, `NO_INPUT` or `FAILED` |
| `error_message` | `STRING` | NULL unless the run failed |
| `source_rows_read` | `MAP<STRING, BIGINT>` | Silver rows used, per source table |
| `rows_excluded` | `MAP<STRING, BIGINT>` | Rows left out, per reason. For example rows without an event date |
| `gold_rows_published` | `BIGINT` | Rows written to the Gold table |
| `gold_table_version` | `BIGINT` | Delta version of the Gold table after the run |
| `checks_passed` | `BOOLEAN` | True when every quality gate passed |
| `failed_checks` | `STRING` | Names of the failed gates, separated by `; `. NULL when all passed |
| `quality_metrics` | `MAP<STRING, BIGINT>` | Table-specific counts. For example the G5 release events left out per reason |

## 8. The source-hour manifest

GH Archive delivers one file per hour. A week of timestamps does not prove that every hour was loaded. G5 compares two 24-hour windows, so one missing hour would make it compare 23 hours with 24 without anyone noticing. The manifest is the proof, hour by hour.

`github_lakehouse.audit.source_hour_manifest` has one row per expected UTC hour.

| Column | Type | Meaning |
|---|---|---|
| `source_hour_utc` | `TIMESTAMP` | Start of the hour. Key |
| `source_date` | `DATE` | The UTC date of the hour |
| `expected_file_name` | `STRING` | For example `2024-02-01-0.json.gz` |
| `bronze_ingested` | `BOOLEAN` | The file has rows in Bronze |
| `bronze_row_count` | `BIGINT` | Rows of that file in Bronze. 0 when the file is not in Bronze |
| `bronze_first_ingested_at` | `TIMESTAMP` | First ingestion time of that file |
| `s3_file_found` | `BOOLEAN` | The raw file was found by the last S3 comparison. NULL when the hour was never compared |
| `s3_line_count` | `BIGINT` | Lines of the raw file, from the last S3 comparison |
| `counts_match` | `BOOLEAN` | Bronze rows equal raw lines. Calculated again on every run. NULL when there is no raw line count to compare with |
| `evidence_level` | `STRING` | `BRONZE_AND_S3` when the raw file was found and counted, otherwise `BRONZE_ONLY` |
| `is_complete` | `BOOLEAN` | The one column G5 reads |
| `checked_at` | `TIMESTAMP` | When the row was written |
| `run_id` | `STRING` | The run that wrote it |

Rules:

- The expected hours are 24 for every date from the first to the last source date in Bronze.
- An hour is complete when its file is in Bronze with at least one row and, where compared, the counts match.
- An hour with zero rows is not complete. A real GH Archive hour has many thousands of events.
- The manifest is replaced completely on every run, whatever `source_date` says. It goes through the same steps as a Gold table: stage, check, publish, audit.
- If hours are missing, there are two honest choices: load them, or let G5 leave out the affected releases and report how many. A partial window is never published as a full one.

The S3 comparison is an option of the notebook. **It is not used in this project** (see "Why the S3 comparison is not used" below). How it works:

- It runs only on request, with the widget `check_s3 = yes` of the manifest notebook. It counts the lines of every raw file and stores them in `gold_staging.source_hour_s3_line_counts`.
- A later run without `check_s3` keeps the S3 evidence that is in the manifest and compares it with the Bronze rows again. So the comparison has to run only once, and a later change in Bronze still shows up as a mismatch.
- If S3 cannot be reached, the notebook prints a warning, keeps the evidence it already has and does not fail. `evidence_level` says for every hour which evidence it rests on.
- `check_s3 = yes` is not allowed together with a test schema. A test never reads S3.

Quality gates on every run:

- The number of rows is 24 times the number of expected days.
- No hour and no file name appears twice, and none is missing.
- `bronze_ingested`, `bronze_row_count`, `evidence_level` and `is_complete` are never NULL.
- No hour is marked complete with 0 rows in Bronze or with different counts.
- Hours that are not complete do not fail a gate. They are a result.

Two more checks in the same notebook. Neither stops the run. Both are stored in `quality_metrics` of the audit row:

- **The global routing check:** Bronze rows whose JSON or event type cannot be read. No Silver notebook would pick them up. The Silver profiling found 0. With a `source_date` only that date is read.
- **Thin hours:** hours with less than 70% of the average rows of the hour before and the hour after. A thin hour stays complete. The notebook lists it and shows its events per minute.

The keys of `quality_metrics` for the manifest: `expected_hours`, `complete_hours`, `incomplete_hours`, `hours_in_bronze`, `hours_compared_with_s3`, `hours_with_count_mismatch`, `unexpected_bronze_files`, `thin_hours`, `bronze_rows_checked_for_type`, `bronze_rows_without_readable_type`, `s3_check_requested`, `s3_check_done`.

Measured on 14 November 2026: all 168 expected files are in Bronze, each loaded once, 38,555,222 rows in total.

The first run of the manifest on the real data, on 15 November 2026:

| Value | Result |
|---|---|
| Status | `SUCCEEDED`, no gate failed |
| Hours in the manifest | 168, on 7 days |
| Complete hours | 168 |
| Bronze rows | 38,555,222 |
| Files in Bronze that no hour expects | 0 |
| Bronze rows without a readable event type | 0 of 38,555,222 |
| Thin hours | 1 |
| Hours compared with S3 | 0. The S3 connection was removed on purpose, see below |

**The thin hour.** The file `2024-02-05-19.json.gz` has 139,013 rows. The hours before and after it have 268,637 and 242,290, so it has 54% of their average. It is the only thin hour of the week. Its events per minute in Bronze:

| Minutes (UTC) | Events per minute |
|---|---|
| 19:00 to 19:23 | About 4,350, the usual rate (3,822 to 4,643) |
| 19:24 to 19:30 | Falling, about 2,900 on average |
| 19:31 to 19:57 | About 520 on average (92 to 1,318) |
| 19:58 and 19:59 | None |

The events get fewer step by step and then stop. A file that was cut off during loading would look different: the usual rate up to one point and nothing after it. So the hour was most likely already thin when GH Archive recorded it. That is a conclusion from the pattern. The proof would be the line count of the raw file, which can no longer be read from Databricks (see below).

At the rate of the first 24 minutes a full hour would have about 261,000 events, so about 122,000 are missing. That is an estimate. It is about 2% of the 5,589,778 Bronze rows of 5 February.

What follows from it:

- The hour stays complete in the manifest. Its file is loaded. Events that GH Archive does not have cannot be loaded from anywhere.
- The counts of every daily Gold table (G1, G2, G3, G4, G6) are slightly low for 5 February, because most events from about 19:24 to 20:00 UTC are missing.
- In G5 a window that covers this hour is slightly low. See section 13, "How to read it".

**Why the S3 comparison is not used.** After the seven days were loaded into Bronze, the S3 connection was removed from Databricks on purpose. Without it, nothing can read the bucket by accident and add to the bill. So the raw files cannot be read from Databricks any more, and that is intended.

- `check_s3` stays blank in this project, in manual runs and in the Job.
- Every hour has the evidence level `BRONZE_ONLY`. The columns `s3_file_found`, `s3_line_count` and `counts_match` are NULL.
- The run of 15 November 2026 was made with `check_s3 = yes`. S3 answered `Forbidden`, the notebook printed its warning and went on, as designed. Its audit row has `s3_check_requested` 1 and `s3_check_done` 0.
- What the manifest rests on: all 168 files are in Bronze, each loaded once, and the Bronze ingestion checked 24 of 24 files on each day while the connection still existed.
- What it does not show: that every Bronze file has exactly as many rows as its raw file has lines.
- The option stays in the notebook for a setup that has the connection, for example if more days are loaded later. The comparison itself has run only locally, on small files. In Databricks only its fallback has run.

## 9. G1 `repository_delivery_metrics`

**Question.** For each repository and day: how many pull request events happened, what share of the closed ones were merged, how long did pull requests take from creation to merge and to close, and how big were they?

| | |
|---|---|
| Source | `silver.pull_request_events` only. Push commit counts are not a substitute for pull request size. |
| Grain | One row per repository per day on which it had at least one pull request event. |
| Key | `repo_id`, `activity_date` |

### Two families of numbers

**Event numbers** count events: a pull request that is closed, reopened and closed again has two close actions. **Pull request numbers** count each pull request once per day, using its latest event of that day as the snapshot. Size metrics use snapshots, so a pull request with four events is not counted four times.

### Rules

- A pull request is identified by `pr_id`. If it is missing, `repo_id` plus `pr_number` is used. If both are missing, the event still counts as an event but not as a pull request.
- Merge share uses rows with the action `closed` and a known `pr_merged`.
- Merge hours use rows with the action `closed`, `pr_merged` true, both times known, and a merge time not earlier than the creation time.
- Close hours use rows with the action `closed`, both times known, and a close time not earlier than the creation time.
- A duration belongs to the day of the close event, even when the pull request was created months earlier.
- The snapshot of a pull request is its latest event of that day. If two events share the latest second, the one with the larger `event_id` is used.
- Each size average uses only the snapshots where that size is known.

**Why the action `closed` is required.** An event carries the state of the pull request at a slightly later moment. In this week 4,904 events that are not `closed` already say `pr_merged = true`, and 5,542 `opened` events already carry a close time. Without the action rule, one merged pull request could be counted twice.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day |
| `pr_event_count` | `BIGINT` | All pull request events that day |
| `pr_action_known_rows` | `BIGINT` | Events with a known action |
| `pr_opened_actions` | `BIGINT` | Events with the action `opened` |
| `pr_closed_actions` | `BIGINT` | Events with the action `closed` |
| `pr_reopened_actions` | `BIGINT` | Events with the action `reopened` |
| `pr_other_action_rows` | `BIGINT` | Events with a known action that is none of the three. Expected 0 |
| `eligible_closed_rows` | `BIGINT` | Closed events with a known merged flag |
| `merged_closed_rows` | `BIGINT` | Of those, the merged ones |
| `merge_share_pct` | `DOUBLE` | 100 × merged ÷ eligible. NULL when eligible is 0 |
| `merged_duration_eligible_rows` | `BIGINT` | Merged close events with usable times |
| `avg_merge_hours` | `DOUBLE` | Average hours from creation to merge |
| `p50_merge_hours` | `DOUBLE` | Median hours from creation to merge |
| `closed_duration_eligible_rows` | `BIGINT` | Close events with usable times |
| `avg_close_hours` | `DOUBLE` | Average hours from creation to close |
| `p50_close_hours` | `DOUBLE` | Median hours from creation to close |
| `pr_size_snapshot_rows` | `BIGINT` | Different pull requests seen that day, one snapshot each |
| `pr_commits_eligible_rows` | `BIGINT` | Snapshots with a known commit count |
| `avg_pr_commits` | `DOUBLE` | Average commits over those snapshots |
| `pr_additions_eligible_rows` | `BIGINT` | Snapshots with a known number of added lines |
| `avg_pr_additions` | `DOUBLE` | Average added lines |
| `pr_deletions_eligible_rows` | `BIGINT` | Snapshots with a known number of deleted lines |
| `avg_pr_deletions` | `DOUBLE` | Average deleted lines |
| `pr_changed_files_eligible_rows` | `BIGINT` | Snapshots with a known number of changed files |
| `avg_pr_changed_files` | `DOUBLE` | Average changed files |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `pr_other_action_rows`, `p50_close_hours`.

### Quality gates on every run

- The key is unique and never NULL.
- 0 ≤ merged ≤ eligible closed ≤ closed actions ≤ known actions ≤ all events.
- Opened + closed + reopened + other = known actions.
- Duration eligible counts are not larger than the counts they come from, and no duration is negative.
- Snapshots ≤ events, and each size eligible count ≤ snapshots.
- The sum of `pr_event_count` equals the Silver row count for the same days.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 700,486 |
| Sum of `pr_event_count` | 2,173,958 |
| Sum of opened, closed, reopened, other | 1,126,806 · 1,031,728 · 15,424 · 0 |
| Sum of `eligible_closed_rows` | 1,031,728 |
| Sum of `merged_closed_rows` | 799,855 (77.53% of the eligible rows) |
| Sum of `merged_duration_eligible_rows` | 799,855 |
| Sum of `closed_duration_eligible_rows` | 1,031,580, which is 148 lower than the closed actions |

### How to read it

- "Of the closed pull request events seen that day, this share was merged." It is not the repository's lifetime acceptance rate.
- Durations describe pull requests closed in this week. Their start can lie long before the week.
- Look at the median before the average. Over the whole week the median time to merge is about 13 minutes and the average is 131 hours, because a few very old pull requests pull the average up.
- 70% of the rows are built from one or two events. A share on them is correct but says little. The count columns allow a reader to set a threshold.
- 0.6% of the pull-request-days have more than one `closed` event, so the share of closed events and the share of closed pull requests are almost the same number.

## 10. G2 `repository_issue_flow`

**Question.** For each repository and day: how many issues were opened, closed and reopened, and how long did the closed ones take from creation to close?

| | |
|---|---|
| Source | `silver.issue_events` only. |
| Grain | One row per repository per day on which it had at least one issue event. |
| Key | `repo_id`, `activity_date` |

### Rules

- Resolution hours use rows with the action `closed`, both times known, and a close time not earlier than the creation time. Silver already sets a close time to NULL when it is earlier than the creation time.
- Resolution time is measured from the original creation, not from the last reopen. An issue closed twice gives two close actions.
- The duration belongs to the day of the close event. It can be far longer than seven days.
- An issue is identified by `issue_id`, otherwise by `repo_id` plus `issue_number`. If both are missing, the event still counts as an event but not as an issue.

**Why the action `closed` is required.** In this week 971 events that are not `closed` carry a close time.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day |
| `issue_event_count` | `BIGINT` | All issue events that day |
| `issue_action_known_rows` | `BIGINT` | Events with a known action |
| `issue_opened_actions` | `BIGINT` | Events with the action `opened` |
| `issue_closed_actions` | `BIGINT` | Events with the action `closed` |
| `issue_reopened_actions` | `BIGINT` | Events with the action `reopened` |
| `issue_other_action_rows` | `BIGINT` | Events with a known action that is none of the three. Expected 0 |
| `issue_close_duration_eligible_rows` | `BIGINT` | Close events with usable times |
| `avg_resolution_hours` | `DOUBLE` | Average hours from creation to close |
| `p50_resolution_hours` | `DOUBLE` | Median hours from creation to close |
| `issues_with_usable_key_rows` | `BIGINT` | Events where the issue can be identified |
| `distinct_issues_observed` | `BIGINT` | Different issues seen that day |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `issue_other_action_rows`.

### Quality gates on every run

- The key is unique and never NULL.
- No count column is NULL.
- Opened + closed + reopened + other = known actions ≤ all events.
- Duration eligible rows ≤ close actions, and no duration is negative.
- A row has an average and a median exactly when it has duration eligible rows.
- Distinct issues ≤ rows with a usable key ≤ all events.
- The sum of `issue_event_count` equals the Silver row count for the same days. The Silver rows are counted directly on the Silver table.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `COUNT_IS_NULL`, `ACTIONS_DO_NOT_ADD_UP`, `KNOWN_ACTIONS_ABOVE_EVENTS`, `ELIGIBLE_ROWS_ABOVE_CLOSED_ACTIONS`, `NEGATIVE_DURATION`, `DURATION_DOES_NOT_FIT_ELIGIBLE_ROWS`, `ISSUE_COUNTS_OUT_OF_ORDER`, `EVENT_SUM_DIFFERS_FROM_SILVER`.

### The audit row

| Column | Content for G2 |
|---|---|
| `source_rows_read` | Key `issue_events`: the Silver rows on the affected days |
| `rows_excluded` | Key `missing_event_date`: rows in the scope of the run without an event date |
| `quality_metrics` | `repository_days` and the sums of eight count columns over the rows of this run: `issue_event_count`, `issue_action_known_rows`, `issue_opened_actions`, `issue_closed_actions`, `issue_reopened_actions`, `issue_other_action_rows`, `issue_close_duration_eligible_rows`, `issues_with_usable_key_rows` |

After a run for all dates, these totals can be compared directly with the table below. `distinct_issues_observed` is not summed, because an issue seen on two days would be counted twice.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 194,627 |
| Sum of `issue_event_count` | 548,956 |
| Sum of opened, closed, reopened, other | 322,476 · 217,898 · 8,582 · 0 |
| Sum of `issue_close_duration_eligible_rows` | 217,866, which is 32 lower than the closed actions |
| Sum of `issues_with_usable_key_rows` | 548,956 |

### Results on the real week

The first two runs of G2 on the real data, on 15 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. 1 affected day, 58,419 Silver rows read, 19,824 rows published with `REPLACE WHERE` |
| All dates | `SUCCEEDED`, no gate failed. 7 affected days, 548,956 Silver rows read, 194,627 rows published with `INSERT OVERWRITE` |

- Every expected value of the table above was met exactly.
- The rows per day are those of section 18.
- The totals of 3 February were the same after both runs.
- No row was without an event date, and no action was other than `opened`, `closed` and `reopened`.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

| Day | Rows | Issue events | Opened | Closed | Reopened | Duration eligible rows |
|---|---|---|---|---|---|---|
| 2024-02-01 | 31,387 | 90,166 | 53,454 | 35,274 | 1,438 | 35,272 |
| 2024-02-02 | 29,315 | 82,090 | 48,488 | 32,349 | 1,253 | 32,340 |
| 2024-02-03 | 19,824 | 58,419 | 35,258 | 22,250 | 911 | 22,250 |
| 2024-02-04 | 21,107 | 58,167 | 35,069 | 22,383 | 715 | 22,382 |
| 2024-02-05 | 30,383 | 83,467 | 47,949 | 34,169 | 1,349 | 34,160 |
| 2024-02-06 | 31,706 | 88,826 | 51,068 | 36,201 | 1,557 | 36,194 |
| 2024-02-07 | 30,905 | 87,821 | 51,190 | 35,272 | 1,359 | 35,268 |
| **Week** | **194,627** | **548,956** | **322,476** | **217,898** | **8,582** | **217,866** |

### How to read it

- The table shows the flow of this week. It does not show how many issues are open in total.
- Close actions are not the same as distinct issues closed. Both numbers are in the table.
- Look at the median before the average. Over the whole week the median time to close is about 4 days and the average is 119 days.
- 77% of the rows are built from one or two events.

## 11. G3 `repository_contributor_concentration`

**Question.** For each repository and day: how many people did the engineering work, and how much of it came from the most active ones?

| | |
|---|---|
| Sources | Six Silver tables: push, pull request, issue, issue comment, pull request review and pull request review comment events. |
| Grain | One row per repository per day with at least one event in the six tables. |
| Key | `repo_id`, `activity_date` |

### Rules

- One engineering activity event is one row from the six tables. Watch, fork and release events are not included.
- An event is attributable when `actor_id` is known. Events without an actor are counted separately and never given to a made-up person.
- All shares use the attributable events as their base.
- The HHI is the sum of the squared shares of all actors, with shares as fractions. It is 1 when one person did everything and moves towards 0 as the work spreads out.
- Every actor is kept, including bots (decision 6).

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day |
| `engineering_activity_events` | `BIGINT` | All events from the six tables that day |
| `attributable_events` | `BIGINT` | Events with a known actor |
| `unattributable_events` | `BIGINT` | Events without a known actor. The two add up to the total |
| `actor_coverage_pct` | `DOUBLE` | 100 × attributable ÷ total |
| `unique_active_actors` | `BIGINT` | Different actors that day. 0 when no event is attributable |
| `top_actor_events` | `BIGINT` | Events of the most active actor. NULL when no event is attributable |
| `top_actor_share_pct` | `DOUBLE` | 100 × top actor events ÷ attributable events |
| `top_3_actor_events` | `BIGINT` | Events of the three most active actors together. NULL when no event is attributable |
| `top_3_actor_share_pct` | `DOUBLE` | 100 × top 3 actor events ÷ attributable events |
| `hhi_actor_activity` | `DOUBLE` | Concentration index from 0 to 1, with 4 decimals. NULL when no event is attributable |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `top_3_actor_events`. Actor IDs are not published in this table, so a tie between equally active actors changes no value.

### Quality gates on every run

- The key is unique and never NULL.
- Attributable + unattributable = all events.
- Actors ≤ attributable events, and top ≤ top 3 ≤ attributable events.
- Shares lie between 0 and 100. The HHI lies between 1 ÷ actors and 1, within rounding (0.0001).
- No `event_id` appears twice in the stacked input.
- The sum of `engineering_activity_events` equals the Silver rows of the six tables for the same days.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 4,318,172 |
| Sum of `engineering_activity_events` | 31,757,399 |
| Sum of `unattributable_events` | 0, so `actor_coverage_pct` is 100.00 on every row |

### How to read it

- A repository with one active person on a day has a top share of 100 by definition. Compare only repository-days with enough events. 59% of the rows are built from one or two events.
- Bots are actors in version 1. Logins that end in `[bot]` produce 17.5% of the engineering events of the week.
- The largest accounts are automation with an ordinary login. The single most active account produced 16.9% of all engineering events in only 30 repositories, and the 20 most active accounts together produced about 48%. A filter on `[bot]` would not remove them. Any list of "most active repositories" is led by automated repositories.
- One event is not one unit of work: a push with 100 commits is one event.
- Pushes are 83.6% of the input, so the result mostly reflects who pushes.

## 12. G4 `repository_collaboration_metrics`

**Question.** For each repository and day: how much reviewing and discussing happened, on what, and by how many people?

| | |
|---|---|
| Sources | Three Silver tables: issue comment, pull request review and pull request review comment events. |
| Grain | One row per repository per day with at least one event in the three tables. |
| Key | `repo_id`, `activity_date` |

### Rules

- One event is one row. In this week every known action in the three tables is `created`, so edits and deletions do not appear. Two issue comment events have no action at all. They still count as events.
- Issue comments are split by `comment_target_type`: `issue`, `pull_request` or unknown. Unknown stays unknown.
- Review events are split by `review_state`. A missing state is not counted as any state.
- People are counted once across all three tables: someone who comments and reviews is one collaborator.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day |
| `issue_comment_events` | `BIGINT` | All issue comment events |
| `issue_conversation_comment_events` | `BIGINT` | Of those, comments on issues |
| `pr_conversation_comment_events` | `BIGINT` | Of those, comments in pull request conversations |
| `unclassified_conversation_comment_events` | `BIGINT` | Of those, comments with an unknown target |
| `review_events` | `BIGINT` | All review events |
| `review_state_known_events` | `BIGINT` | Reviews with a known state |
| `review_approved_events` | `BIGINT` | Reviews with the state `approved` |
| `review_changes_requested_events` | `BIGINT` | Reviews with the state `changes_requested` |
| `review_commented_events` | `BIGINT` | Reviews with the state `commented` |
| `review_dismissed_events` | `BIGINT` | Reviews with the state `dismissed` |
| `review_other_state_events` | `BIGINT` | Known state that is none of the four, for example `pending` |
| `inline_review_comment_events` | `BIGINT` | All inline review comment events |
| `collaboration_events_total` | `BIGINT` | Sum of the three event types |
| `actor_known_events` | `BIGINT` | Events with a known actor |
| `unique_collaborating_actors` | `BIGINT` | Different actors across the three tables |
| `collaborator_actor_coverage_pct` | `DOUBLE` | 100 × actor-known ÷ total |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `review_dismissed_events`, `review_other_state_events`, `actor_known_events`.

### Quality gates on every run

- The key is unique and never NULL.
- The three target columns add up to `issue_comment_events`.
- The five state columns add up to `review_state_known_events`, which is ≤ `review_events`.
- The total equals the sum of the three event types.
- Unique actors ≤ actor-known events ≤ total.
- No `event_id` appears twice in the stacked input.
- The count of each event type equals the Silver row count of its table for the same days.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 444,720 |
| Sum of `collaboration_events_total` | 2,497,518 (1,357,324 + 717,277 + 422,917) |
| Sum of `unclassified_conversation_comment_events` | 2 |
| Sum of `review_state_known_events` | 717,277, because no review has a missing state |

### How to read it

- The numbers count events. An approving review event is not the same as "the pull request was approved".
- More comments do not mean better collaboration.
- Most issue comment events are pull request conversations, about 62% in this week.
- 38% of the issue comment events come from logins that end in `[bot]`.

## 13. G5 `release_activity_shift`

**Question.** For each eligible release: how many activity events did the repository have in the 24 hours after the release, compared with the 24 hours before?

| | |
|---|---|
| Sources | `silver.release_events` for the anchors. Push, pull request, issue, watch and fork events for the surrounding activity. `audit.source_hour_manifest` for coverage. Issue comments and reviews are not G5 sources. |
| Grain | One row per eligible release. This release is called the anchor. |
| Key | `repo_id`, `release_anchor_key` |

### Anchor rules

A release event becomes an anchor only if it passes all seven rules. The rules are applied in this order. An event that fails is counted once, under the first rule it fails, and the counts go to `quality_metrics` in the audit row. The release event itself stays in Silver and in G6.

| # | A release event becomes an anchor only if | Otherwise it is counted as | Expected |
|---|---|---|---|
| 1 | `release_action` is `published` | `NOT_PUBLISHED` | 0 |
| 2 | `release_draft` is known and false | `DRAFT_OR_UNKNOWN` | 278 |
| 3 | `release_published_at` is known | `MISSING_PUBLISHED_AT` | 0 |
| 4 | `release_prerelease` is known | `PRERELEASE_UNKNOWN` | 0 |
| 5 | `release_id` is known | `MISSING_RELEASE_ID` | 0 |
| 6 | it is the earliest event of its release, among the events that passed rules 1 to 5 | `REPEATED_RELEASE_EVENT` | 740 |
| 7 | every source hour touched by its 48-hour window is complete in the manifest | `WINDOW_NOT_COVERED` | 50,103 |
| | **Anchor** | | **108,295** |

Notes on the rules:

- **Rule 2 before rule 3.** The 278 drafts of this week are exactly the 278 release events without a published time. A draft has no published time, so "draft" is the real reason and is checked first.
- **Rule 6.** A release is identified by `repo_id` plus `release_id`. "Earliest" means the smallest `event_timestamp`. If two events share it, the smaller `event_id` is used. An event that already failed an earlier rule never takes the place of a valid one.
- **Rule 6 is not about copies.** Of the 408 releases with more than one event, 318 carry different published times. They are rolling releases, for example with the tag `nightly`, which are published again under the same release ID. Only their first publication in the week becomes an anchor.
- **Rule 7** was measured by time only: a release fits when it was published from 2024-02-02 00:00:00 to 2024-02-07 00:00:00 UTC, both included. The real run also asks the manifest whether every hour of the two windows is complete.
- A release tag is a label. The week has 158,588 releases but only 140,971 different tags per repository, so tags are reused.

### Window rules

- r is `release_published_at`. It is never replaced by the event time or the ingestion time.
- The before window runs from r − 24 hours up to, but not including, r. The after window runs from r up to, but not including, r + 24 hours.
- An event exactly at r counts in the after window. An event exactly at r + 24 hours is outside both windows.
- Rule 7 needs 48 complete hourly files when r is exactly on the hour, otherwise 49.
- Activity is matched by `repo_id` and `event_timestamp`. A row without an event time cannot be placed and is counted as left out.
- A window without activity gives 0. That is a measured zero, because the window is fully covered.
- Push commit counts and pull request commit counts are never added together.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `release_anchor_key` | `STRING` | Key. The `release_id` written as text |
| `repo_name` | `STRING` | Label: the name on the anchor event |
| `release_id` | `BIGINT` | The release ID |
| `release_tag` | `STRING` | Label. Can be missing |
| `release_published_at` | `TIMESTAMP` | The anchor time r |
| `is_prerelease` | `BOOLEAN` | True or false, never NULL |
| `anchor_event_id` | `STRING` | The Silver event the anchor comes from |
| `window_start_at` | `TIMESTAMP` | r − 24 hours |
| `window_end_at` | `TIMESTAMP` | r + 24 hours |
| `activity_events_before` | `BIGINT` | Events of all five types in the before window |
| `activity_events_after` | `BIGINT` | Events of all five types in the after window |
| `activity_events_change` | `BIGINT` | After − before |
| `activity_events_pct_change` | `DOUBLE` | 100 × change ÷ before. NULL when before is 0 |
| `baseline_zero_flag` | `BOOLEAN` | True when before is 0 |
| `push_events_before`, `push_events_after` | `BIGINT` | Push events in each window |
| `pr_events_before`, `pr_events_after` | `BIGINT` | Pull request events in each window |
| `issue_events_before`, `issue_events_after` | `BIGINT` | Issue events in each window |
| `watch_events_before`, `watch_events_after` | `BIGINT` | Watch events in each window |
| `fork_events_before`, `fork_events_after` | `BIGINT` | Fork events in each window |
| `push_commit_eligible_events_before`, `push_commit_eligible_events_after` | `BIGINT` | Push events with a known commit count |
| `known_push_commits_before`, `known_push_commits_after` | `BIGINT` | Sum of the known push commit counts. 0 when no push event has a known count |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

### Quality gates on every run

- The key is unique, and the row count equals the number of eligible anchors.
- Before and after totals equal the sum of their five type columns.
- Change = after − before. The percent change is NULL exactly when before is 0.
- Every window lies inside complete hours.
- Commit-eligible push events ≤ push events.
- Eligible anchors plus all failure reasons equal the number of release events.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 108,295, when every hour is complete in the manifest |
| Rows with `is_prerelease` true | 13,586 |
| Rows + all failure reasons | 159,416 |

### How to read it

- It is a before-and-after comparison, not proof that the release caused the change.
- Windows of releases that are close together overlap. Do not add rows up across releases.
- The day of the week and the time of day also change activity.
- Releases from the first and last 24 hours of the week are not in the table.
- One source hour is thin: on 5 February 2024 from about 19:24 to 20:00 UTC, Bronze holds far fewer events than usual (section 8). A release published in the 24 hours before these minutes has them in its after window, so its after count is slightly low and the change looks more negative than it was. A release published in the 24 hours after them, until 6 February 20:00 UTC, has them in its before window, so its before count is slightly low and the change looks more positive than it was. Over all event types and repositories, about 2% of the events of such a 24-hour window are missing. That is an estimate, and for one repository it can be more or less.
- A release often comes with its own pushes, which count as activity.
- Many releases are published by automation. 7,416 of the 62,125 repositories with releases have no activity event in the whole week, so many rows show 0 before and 0 after, an empty percentage and `baseline_zero_flag` true. That is a real result.

## 14. G6 `repository_activity_profile`

**Question.** For each repository and day: how much activity was there, and how is it split over the nine event types?

| | |
|---|---|
| Sources | All nine Silver tables. |
| Grain | One row per repository per day with at least one retained event. |
| Key | `repo_id`, `activity_date` |

### Rules

- Each event type is one category, and each event is in exactly one category. The nine counts therefore add up to the total.
- A type without events has the count 0 and the share 0. That is a measured zero.
- Shares use the total as their base. The total is never 0, because a row exists only when there is at least one event.
- Broader groups, such as "review" for reviews plus inline comments, are not stored. A report can add them from the nine counts.

### Core columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day |
| `all_retained_event_count` | `BIGINT` | All events of the nine types that day |
| `push_events` | `BIGINT` | Push events |
| `pull_request_events` | `BIGINT` | Pull request events |
| `issue_events` | `BIGINT` | Issue events |
| `issue_comment_events` | `BIGINT` | Issue comment events |
| `review_events` | `BIGINT` | Pull request review events |
| `inline_review_comment_events` | `BIGINT` | Pull request review comment events |
| `watch_events` | `BIGINT` | Watch events |
| `fork_events` | `BIGINT` | Fork events |
| `release_events` | `BIGINT` | Release events |
| `push_share_pct`, `pull_request_share_pct`, `issue_share_pct`, `issue_comment_share_pct`, `review_share_pct`, `inline_review_comment_share_pct`, `watch_share_pct`, `fork_share_pct`, `release_share_pct` | `DOUBLE` | One share per type: 100 × count ÷ total |
| `actor_known_events` | `BIGINT` | Events with a known actor |
| `unique_known_actors` | `BIGINT` | Different actors across all nine types |
| `actor_attribution_coverage_pct` | `DOUBLE` | 100 × actor-known ÷ total |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

### Optional columns, added in a second step

They are placed after the core columns and before the two technical columns.

| Column | Type | Meaning |
|---|---|---|
| `active_utc_hours` | `BIGINT` | Number of different UTC hours with at least one event, from 1 to 24 |
| `push_commit_eligible_events` | `BIGINT` | Push events with a known commit count |
| `known_push_commits` | `BIGINT` | Sum of those commit counts |
| `push_distinct_commit_eligible_events` | `BIGINT` | Push events with a known distinct commit count |
| `known_push_distinct_commits` | `BIGINT` | Sum of those distinct commit counts |
| `issue_conversation_comment_events` | `BIGINT` | Comments on issues, as in G4 |
| `pr_conversation_comment_events` | `BIGINT` | Comments in pull request conversations, as in G4 |
| `unclassified_conversation_comment_events` | `BIGINT` | Comments with an unknown target, as in G4 |

In this week 611 push events have no usable commit counts.

### Quality gates on every run

- The key is unique and never NULL.
- The nine counts add up to the total.
- The nine shares add up to 100, within rounding (99.95 to 100.05).
- Unique actors ≤ actor-known events ≤ total.
- No `event_id` appears twice in the stacked input.
- The count of each event type equals the Silver row count of its table for the same days.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 5,000,573 |
| Sum of `all_retained_event_count` | 33,623,791 |
| Sum of each type column | The row count of its Silver table in section 1 |
| `actor_attribution_coverage_pct` | 100.00 on every row |

### How to read it

- The table shows a mix of event counts. It does not measure the amount of work or the health of a project.
- Watch events are new stars only. Removed stars are not in the data.
- Pushes are 78.9% of all events, so most repository-days are mostly pushes.
- 63% of the rows are built from one or two events.
- The repositories with the most events are automated. One repository has 64,783 events on a single day.

## 15. The final Gold audit

`09_gold_final_audit` runs after the six tables. It writes one audit row and fails its task if any check fails.

| Check | Expected on the full week |
|---|---|
| Every Gold table: key unique and never NULL | 0 violations |
| G1: rows and sum of `pr_event_count` | 700,486 and 2,173,958 |
| G2: rows and sum of `issue_event_count` | 194,627 and 548,956 |
| G3: rows and sum of `engineering_activity_events` | 4,318,172 and 31,757,399 |
| G4: rows and sum of `collaboration_events_total` | 444,720 and 2,497,518 |
| G6: rows and sum of `all_retained_event_count` | 5,000,573 and 33,623,791 |
| G6 `pull_request_events` = G1 `pr_event_count`, for every repository-day | Equal |
| G6 `issue_events` = G2 `issue_event_count`, for every repository-day | Equal |
| G6 issue comments + reviews + inline comments = G4 total | Equal |
| G6 six engineering types = G3 total | Equal |
| Actors in G6 ≥ actors in G3 ≥ actors in G4, for every repository-day | True |
| Every repository-day of G1 to G4 exists in G6 | True |
| Every G5 repository exists in G6 | True |
| G5 rows + G5 failure reasons | 159,416, the number of release events |

For a run with one date, the audit compares the same days on both sides instead of the week totals. The audit compares Gold with Silver and Gold with Gold. The fixed numbers above are written here for a person to compare with, they are not built into the code.

## 16. Tests

Each Gold notebook has a test notebook in `Notebooks/Gold_layer/tests/`.

1. The test creates a test schema with empty copies of the Silver tables the notebook reads.
2. It inserts a few hand-made rows. The correct Gold result for them is worked out by hand and written into the test.
3. It runs the Gold notebook with `test_schema` set, so production tables are never touched.
4. It compares the Gold table with the expected result, value by value, and prints PASS or FAIL for each check.

| Test | What it proves |
|---|---|
| Schema | The Gold table has exactly the columns and types of this file. |
| Exact values | Hand-made rows give the hand-calculated result. |
| Missing values | A NULL input is left out of that one metric, and its eligible-rows column is one lower. |
| Zero base | The percentage or average is NULL, not 0. |
| Rerun | Running again changes nothing. |
| Late event | Adding one Silver row and rerunning changes the right Gold row. |
| Removed event | Removing Silver rows and rerunning lowers or removes the Gold row. |
| No input | A date without Silver rows gives the status `NO_INPUT` and leaves Gold unchanged. |
| Other days untouched | A run for one source date does not rewrite the rows of the other days. |
| A day that is gone | After all Silver rows of a day are removed, a run for all dates removes that day from Gold. |
| Failed check | Bad toy data makes a gate fail: Gold stays unchanged and the status is `FAILED`. |
| Bad parameter | An invalid `source_date`, `test_schema` or `run_id` stops the notebook before any write. |

A test notebook runs the real notebook with `%run`. A cell that is expected to fail says so in the text before it. "Run all" stops at such a cell, and the test goes on with "Run all below" from the cell after it. The checks count runs, not audit rows, so it does no harm if such a cell runs twice.

Every check section writes its number of failed checks into one dictionary. The cell "Result of the whole test" near the end reads it and prints `TEST PASSED` only when every section ran and every check passed. A section that failed or was left out is named.

A result counts as verified only after the notebook has run in the Databricks workspace.

## 17. Code style

The Gold notebooks are written so that a junior data engineer can read every line and explain it.

- The calculations are written in SQL. Each step is one query, run with `spark.sql`. Python is used only for the parameters, the quality gates and the audit row.
- One step per cell. A text cell before it says what the step does and why.
- Plain SQL only: `SELECT`, `WHERE`, `GROUP BY`, `JOIN` and `UNION ALL`, plus `ROW_NUMBER` where a rule asks for the latest or the earliest row. No helper functions, no loops that build tables, and no shortened one-line tricks.
- Names say what a thing is, for example `closed_events` and not `df2`.
- Every number in a result can be traced to one visible step.
- Simple does not mean less. Every rule, quality gate, audit column and test of this file stays.

## 18. Measured facts

These numbers come from the run of `00_gold_data_profiling` on 14 November 2026. All 46 result tables were produced and no cell failed.

### Rows per day

The row counts a run for one date should produce.

| Day | G1 rows | G2 rows | G3 rows | G4 rows | G6 rows | G6 events |
|---|---|---|---|---|---|---|
| 2024-02-01 | 116,405 | 31,387 | 677,026 | 75,896 | 780,577 | 4,931,231 |
| 2024-02-02 | 113,664 | 29,315 | 634,118 | 67,275 | 735,149 | 4,944,140 |
| 2024-02-03 | 68,242 | 19,824 | 495,687 | 37,985 | 579,908 | 4,447,005 |
| 2024-02-04 | 62,025 | 21,107 | 507,896 | 37,587 | 602,850 | 4,441,124 |
| 2024-02-05 | 122,282 | 30,383 | 665,685 | 82,181 | 763,841 | 4,789,485 |
| 2024-02-06 | 111,296 | 31,706 | 677,412 | 73,223 | 777,905 | 4,999,343 |
| 2024-02-07 | 106,572 | 30,905 | 660,348 | 70,573 | 760,343 | 5,071,463 |
| **Week** | **700,486** | **194,627** | **4,318,172** | **444,720** | **5,000,573** | **33,623,791** |

### Keys and warnings

| Fact | Value |
|---|---|
| Silver rows without event time, event date, repository ID or actor ID | 0 in all nine tables |
| Rows where `event_date` differs from `_source_date` | 0 |
| `event_id` values that appear in more than one Silver table | 0 |
| Pull request events without a usable key | 0 of 2,173,958 |
| Issue events without a usable key | 0 of 548,956 |
| Release events without a release ID | 0 of 159,416 |
| Warning `MISSING: pr_closed_at` | 148 rows |
| Warning `MISSING: issue_closed_at` | 32 rows |
| Warning `DISTINCT_COMMIT_COUNT_EXCEEDS_TOTAL` | 611 push rows |
| Warning `MISSING: release_published_at` | 278 rows |
| Issue comment rows without action, issue ID, issue number, comment ID and target type | 2 |

### Entities

| Fact | Value |
|---|---|
| Different pull requests | 1,393,696 |
| Different issues | 444,569 |
| Different releases | 158,588 |
| Repositories with any event | 2,713,169 |
| Repositories with a release event | 62,125 |
| Repositories with more than one name in the week | 18,436 |
| Repository-days with more than one name on the same day | 13,514 of 5,000,573 |
| Names used by more than one repository ID | 24,041 |

### Durations over the whole week

The medians in this table are estimates from the profiling run. The Gold tables calculate exact medians.

| Duration | Eligible rows | Median hours | Average hours |
|---|---|---|---|
| Pull request, creation to merge | 799,855 | 0.22 | 130.72 |
| Pull request, creation to close | 1,031,580 | 0.77 | 491.96 |
| Issue, creation to close | 217,866 | 95.29 | 2,864.22 |

### Releases

| Fact | Value |
|---|---|
| Release events: normal, prerelease, draft | 138,682 · 20,456 · 278 |
| Releases with more than one event | 408 |
| Of those, with different published times | 318 |
| Published in the first 24 hours of the data | 26,182 events |
| Published in the last 24 hours of the data | 24,079 events |
| Published where both windows fit | 108,877 events |
| Join of releases and activity before the time filter | 7,325,231 pairs. Largest single repository: 374,302 |

## 19. Change log

| Date | Change |
|---|---|
| 2026-11-14 | First version. The 13 decisions were approved and the profiling results were worked in. Compared with the plan: G5 checks the draft rule before the published-time rule, rule 6 picks the earliest event among those that passed the earlier rules, and the reason code `DUPLICATE_RELEASE_EVENT` is now `REPEATED_RELEASE_EVENT`. Code style: the calculations are written in SQL and in a style a junior data engineer can read. |
| 2026-11-15 | Step 4, the foundation. Section 8: the S3 comparison is requested with `check_s3 = yes` and its evidence is kept by later runs, `evidence_level` is decided per hour, and the manifest has its own quality gates, the thin-hour check and the keys of `quality_metrics`. Section 4: name of a staging copy in a test schema. Section 6: allowed characters of `run_id`. |
| 2026-11-15 | First run of the manifest in Databricks. Section 8: the results of the run, the thin hour minute by minute (most likely thin at the source, the hour stays complete), and why the S3 comparison is not used: the S3 connection was removed on purpose after the week was loaded, so decision 11 now rests on Bronze alone. Section 13: note on windows that cover the thin hour. Section 16: a test notebook ends with one result for the whole test. |
| 2026-11-15 | Status: the test notebook of the manifest passed in Databricks (12 of 12 sections, 145 checks). No rule changed. |
| 2026-11-15 | Step 5, G2. Section 5: `event_id` is compared as text when two events share the latest second, and a repository-day without any name has the label NULL. Section 6: rows without an event date are counted under `missing_event_date`, the two publish statements, and `NO_INPUT` when there is no affected day. Section 10: two more quality gates (no count is NULL, durations fit the eligible rows), the names of the gates, and the content of the audit row. Section 16: three more kinds of test (other days untouched, a day that is gone, a bad `run_id`). |
| 2026-11-15 | First runs of G2 in Databricks. Status and section 10: the test notebook passed (13 of 13 sections, 135 checks), and the runs for one date and for all dates met every expected value. Section 10 has the results per day. No rule changed. |
