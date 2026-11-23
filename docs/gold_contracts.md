# Gold layer contract

This file is the single written source of truth for the six Gold tables of the GitHub Activity Lakehouse.

- It fixes what each table means, which columns it has, which rows a metric may use and which checks every run must pass.
- **A rule or a column changes in this file first, and in code second.**
- It contains no code. The notebooks in `Notebooks/Gold_layer/` implement it.

| | |
|---|---|
| Data window | 2024-02-01 00:00 UTC up to, but not including, 2024-02-08 00:00 UTC (168 hourly GH Archive files) |
| Based on | Completion Manual (Part IV and V), Silver Schema Reference, revised NULL and Quarantine Policy, the Silver notebooks on `main`, and the run of `00_gold_data_profiling` on 14 November 2026 |
| Status | Rules and decisions approved on 14 November 2026. Written so far: the Gold setup (`08`), the source-hour manifest (`07`) and all six Gold tables, G2 (`02`), G1 (`01`), G4 (`04`), G6 (`06`), G3 (`03`) and G5 (`05`), and the final Gold audit (`09`). The manifest ran on the real data on 15 November 2026: 168 hours, all complete. Its test notebook passed in Databricks on the same day: 12 of 12 sections, 145 checks. G2 ran on the real data on 15 November 2026 (UTC): 194,627 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 135 checks. G1 ran on the real data on 16 November 2026 (UTC): 700,486 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 153 checks. G4 ran on the real data on 17 November 2026 (UTC): 444,720 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 158 checks. G6 (core columns) ran on the real data on 17 November 2026 (UTC): 5,000,573 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 180 checks. The optional columns of G6 ran on the real data on 17 November 2026 (UTC), every expected value met. The extended test notebook passed on the same day: 13 of 13 sections, 208 checks. G3 ran on the real data on 22 November 2026 (UTC): 4,318,172 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 174 checks. G5 ran on the real data on 22 November 2026 (UTC): 108,295 rows, every expected value met. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 179 checks. All six Gold tables are built. The final Gold audit (`09`) ran on the real data on 23 November 2026 (UTC): 33 of 33 checks passed, with and without a source date. Its test notebook passed in Databricks on the same day: 13 of 13 sections, 176 checks |

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
| 1 | Gold reads only Silver. | Never Bronze and never another Gold table. The one addition is G5, which also reads the source-hour manifest. The final audit (section 15) reads the six Gold tables to check them, and builds no table. |
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

1. **Scope.** `processing_scope` is always `ALL_DATES`. A valid `source_date` is checked and written into the audit row, but it changes nothing in what the run reads or writes. A source date without Silver rows therefore still rebuilds the whole table and is not `NO_INPUT`.
2. **Build, stage and check** as for the daily tables, from all Silver rows and the manifest.
3. **Publish** with `INSERT OVERWRITE`, as one Delta commit. A release that is no longer an anchor disappears.
4. **Audit.** `affected_dates` lists the event dates of the release events. G5 has no days of its own.

The status is `NO_INPUT` only when Silver has no release event at all. When there are release events but none of them is an anchor, the run succeeds and replaces the table with an empty one, and the notebook prints a warning. The usual reason is a manifest whose hours are not complete.

### Status

| Status | Meaning |
|---|---|
| `SUCCEEDED` | The gates passed and the result was published. |
| `NO_INPUT` | Silver has no rows in the scope of this run. Gold is unchanged and the task does not fail. |
| `FAILED` | A gate or a write failed. Nothing was published. The audit row is written first, then the notebook raises an error so that the Job task fails. |

A run with a blank date replaces the whole table. It is also the way to remove a day that no longer exists in Silver at all.

## 7. The Gold audit table

`github_lakehouse.audit.gold_pipeline_runs` gets exactly one row per run of a Gold notebook. The manifest notebook writes its row here too, with the manifest as `gold_table`. The final audit writes its row here too, with `gold_table` NULL, because it writes no table (section 15).

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
- A pull request number counts only inside its repository. The same number in two repositories is two pull requests.
- Merge share uses rows with the action `closed` and a known `pr_merged`.
- Merge hours use rows with the action `closed`, `pr_merged` true, both times known, and a merge time not earlier than the creation time.
- Close hours use rows with the action `closed`, both times known, and a close time not earlier than the creation time.
- A duration belongs to the day of the close event, even when the pull request was created months earlier.
- The snapshot of a pull request is its latest event of that day. If two events share the latest second, the one with the larger `event_id` is used.
- Each size average uses only the snapshots where that size is known. A size of 0 is a known value.
- A repository-day whose events all have no pull request key still has a row. Its snapshot count and its four size eligible counts are 0, and its four size averages are NULL.
- An event with an unknown action counts as an event, but in none of the four action columns.

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
- No count column is NULL.
- Opened + closed + reopened + other = known actions ≤ all events.
- Merged ≤ eligible closed ≤ closed actions.
- The merge share lies between 0 and 100.
- Merge duration eligible rows ≤ merged closes, close duration eligible rows ≤ closed actions, and no duration is negative.
- Snapshots ≤ events, and each size eligible count ≤ snapshots.
- A row has a share, an average or a median exactly when the count it is built from is above 0.
- The sum of `pr_event_count` equals the Silver row count for the same days. The Silver rows are counted directly on the Silver table.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `COUNT_IS_NULL`, `ACTIONS_DO_NOT_ADD_UP`, `KNOWN_ACTIONS_ABOVE_EVENTS`, `CLOSED_COUNTS_OUT_OF_ORDER`, `SHARE_OUT_OF_RANGE`, `DURATION_ROWS_ABOVE_THEIR_BASE`, `NEGATIVE_DURATION`, `SNAPSHOT_COUNTS_OUT_OF_ORDER`, `VALUE_WITHOUT_ELIGIBLE_ROWS`, `ELIGIBLE_ROWS_WITHOUT_VALUE`, `EVENT_SUM_DIFFERS_FROM_SILVER`.

### The audit row

| Column | Content for G1 |
|---|---|
| `source_rows_read` | Key `pull_request_events`: the Silver rows on the affected days |
| `rows_excluded` | Key `missing_event_date`: rows in the scope of the run without an event date |
| `quality_metrics` | `repository_days` and the sums of the fifteen count columns over the rows of this run: `pr_event_count`, `pr_action_known_rows`, `pr_opened_actions`, `pr_closed_actions`, `pr_reopened_actions`, `pr_other_action_rows`, `eligible_closed_rows`, `merged_closed_rows`, `merged_duration_eligible_rows`, `closed_duration_eligible_rows`, `pr_size_snapshot_rows`, `pr_commits_eligible_rows`, `pr_additions_eligible_rows`, `pr_deletions_eligible_rows`, `pr_changed_files_eligible_rows` |

After a run for all dates, these totals can be compared directly with the table below. The sum of `pr_size_snapshot_rows` counts pull-request-days: a pull request seen on two days is counted once on each of them.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 700,486 |
| Sum of `pr_event_count` | 2,173,958 |
| Sum of `pr_action_known_rows` | 2,173,958: the three actions below add up to all events |
| Sum of opened, closed, reopened, other | 1,126,806 · 1,031,728 · 15,424 · 0 |
| Sum of `eligible_closed_rows` | 1,031,728 |
| Sum of `merged_closed_rows` | 799,855 (77.53% of the eligible rows) |
| Sum of `merged_duration_eligible_rows` | 799,855 |
| Sum of `closed_duration_eligible_rows` | 1,031,580, which is 148 lower than the closed actions |
| Sum of `pr_size_snapshot_rows` | Not measured before the first run. It lies between 1,393,696, the different pull requests of the week, and 2,173,958, the events |

Rows and events per day, for a run with one date:

| Day | Rows | Pull request events |
|---|---|---|
| 2024-02-01 | 116,405 | 387,295 |
| 2024-02-02 | 113,664 | 332,523 |
| 2024-02-03 | 68,242 | 197,802 |
| 2024-02-04 | 62,025 | 189,714 |
| 2024-02-05 | 122,282 | 388,940 |
| 2024-02-06 | 111,296 | 345,716 |
| 2024-02-07 | 106,572 | 331,968 |
| **Week** | **700,486** | **2,173,958** |

### Results on the real week

The first two runs of G1 on the real data, on 16 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. 1 affected day, 197,802 Silver rows read, 68,242 rows published with `REPLACE WHERE` |
| All dates | `SUCCEEDED`, no gate failed. 7 affected days, 2,173,958 Silver rows read, 700,486 rows published with `INSERT OVERWRITE` |

- Every expected value of the table above was met exactly, and so were the rows and the events of every day.
- The totals of 3 February were the same after both runs.
- No row was without an event date, and no action was other than `opened`, `closed` and `reopened`.
- On every day, all closed events have a known merged flag, and all merged closes have usable times.
- The sum of `pr_size_snapshot_rows` is 1,525,648 pull-request-days. Every snapshot has all four sizes, so the four size eligible counts are 1,525,648 too.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

| Day | Rows | Pull request events | Opened | Closed | Reopened | Merged closes | Close duration eligible rows | Snapshots |
|---|---|---|---|---|---|---|---|---|
| 2024-02-01 | 116,405 | 387,295 | 206,871 | 178,201 | 2,223 | 136,271 | 178,201 | 276,726 |
| 2024-02-02 | 113,664 | 332,523 | 174,936 | 154,455 | 3,132 | 120,720 | 154,412 | 235,368 |
| 2024-02-03 | 68,242 | 197,802 | 101,363 | 94,010 | 2,429 | 74,475 | 94,010 | 131,676 |
| 2024-02-04 | 62,025 | 189,714 | 93,486 | 94,198 | 2,030 | 74,776 | 94,197 | 123,335 |
| 2024-02-05 | 122,282 | 388,940 | 205,940 | 181,136 | 1,864 | 133,185 | 181,085 | 283,667 |
| 2024-02-06 | 111,296 | 345,716 | 174,583 | 169,107 | 2,026 | 133,697 | 169,061 | 242,843 |
| 2024-02-07 | 106,572 | 331,968 | 169,627 | 160,621 | 1,720 | 126,731 | 160,614 | 232,033 |
| **Week** | **700,486** | **2,173,958** | **1,126,806** | **1,031,728** | **15,424** | **799,855** | **1,031,580** | **1,525,648** |

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

The notebook `03_repository_contributor_concentration` builds this table. It places the six tables under each other, as G6 does with nine. They are never joined, because a join of raw events from two tables would multiply the rows.

### Rules

- One engineering activity event is one row from the six tables, and it is counted once. Watch, fork and release events are not included. The notebook does not read their tables at all: they do not count, their actors are not people of this table, they do not give the label, and a new watch, fork or release event does not make its day an affected day.
- An event is attributable when `actor_id` is known. Events without an actor are counted separately and never given to a made-up person.
- All shares and the HHI use the attributable events as their base, not all events. With 5 events of one person, 3 of another and 2 without an actor, the top share is 5 of 8 = 62.5.
- The numbers are counted in two levels: first the events of each person on the repository-day, then, from those, the repository-day. People are counted once across the six tables: someone who pushes and reviews on the same day is one person.
- The people of a repository-day are ranked by their events, the most active first, for each repository and day on its own. The top 3 are the three people with the most events, together. With three people or fewer, the top 3 are everybody.
- When two people have the same number of events, the smaller `actor_id` is ranked first. Actor IDs are not published, and which of the two is first changes no value in the table.
- The HHI is the sum of the squared shares of all people, with the shares as fractions. It is calculated as (the sum of each person's events squared) ÷ attributable events². It is 1 when one person did everything and moves towards 0 as the work spreads out. With n people it is at least 1 ÷ n.
- A repository-day without any attributable event has 0 people and a coverage of 0. Its top counts, shares and HHI are NULL, because there is nobody to rank.
- Every row has at least one event, so the coverage is never NULL.
- Every value is calculated from the exact counts and rounded only once: the shares and the coverage to 2 decimals, the HHI to 4.
- Every actor is kept, including bots (decision 6).
- The `repo_name` label is the name of the latest event with a name across the six tables, see section 5.

### Columns

| Column | Type | Meaning |
|---|---|---|
| `repo_id` | `BIGINT` | Key |
| `activity_date` | `DATE` | Key. The UTC event date |
| `repo_name` | `STRING` | Label: the latest name seen that day in the six tables |
| `engineering_activity_events` | `BIGINT` | All events from the six tables that day |
| `attributable_events` | `BIGINT` | Events with a known actor |
| `unattributable_events` | `BIGINT` | Events without a known actor. The two add up to the total |
| `actor_coverage_pct` | `DOUBLE` | 100 × attributable ÷ total |
| `unique_active_actors` | `BIGINT` | Different actors that day. 0 when no event is attributable |
| `top_actor_events` | `BIGINT` | Events of the most active actor. NULL when no event is attributable |
| `top_actor_share_pct` | `DOUBLE` | 100 × top actor events ÷ attributable events. NULL when no event is attributable |
| `top_3_actor_events` | `BIGINT` | Events of the three most active actors together. NULL when no event is attributable |
| `top_3_actor_share_pct` | `DOUBLE` | 100 × top 3 actor events ÷ attributable events. NULL when no event is attributable |
| `hhi_actor_activity` | `DOUBLE` | Concentration index from 0 to 1, with 4 decimals. NULL when no event is attributable |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `top_3_actor_events`. Actor IDs are not published in this table, so a tie between equally active actors changes no value.

### Quality gates on every run

- The key is unique and never NULL.
- No count column is NULL: all events, attributable, unattributable and people.
- Attributable + unattributable = all events.
- People ≤ attributable events, and a row with attributable events has at least one person.
- For a row with people: 1 ≤ top ≤ top 3 ≤ attributable events, the top actor has at least as many events as the average person (top × people ≥ attributable events), and with three people or fewer the top 3 hold all attributable events.
- A row with attributable events has all five concentration values. A row without attributable events has none of them.
- The two shares lie between 0 and 100. The HHI lies between 1 ÷ people and 1, within rounding (0.0001).
- The coverage lies between 0 and 100, and a row has a coverage exactly when it has events.
- No `event_id` appears twice in the stacked input. This check reads the stacked Silver rows of the affected days, because the Gold rows no longer carry event IDs.
- The sum of `engineering_activity_events` equals the Silver rows of the six tables for the same days. The Silver rows are counted directly on the Silver tables.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `COUNT_IS_NULL`, `ATTRIBUTION_DOES_NOT_ADD_UP`, `ACTOR_COUNTS_OUT_OF_ORDER`, `TOP_COUNTS_OUT_OF_ORDER`, `CONCENTRATION_DOES_NOT_FIT_ITS_BASE`, `SHARE_OUT_OF_RANGE`, `HHI_OUT_OF_RANGE`, `COVERAGE_OUT_OF_RANGE`, `COVERAGE_DOES_NOT_FIT_ITS_BASE`, `EVENT_ID_NOT_UNIQUE`, `EVENT_SUM_DIFFERS_FROM_SILVER`.

This table has no column per kind of event, so `EVENT_SUM_DIFFERS_FROM_SILVER` compares one sum: all events of the staged rows against the Silver rows of the six tables together.

Two gates can be set off by a Silver row that Silver itself would never write: `KEY_IS_NULL` (a row without `repo_id`) and `EVENT_ID_NOT_UNIQUE` (one event in two tables). A row without `repo_id` also sets off `CONCENTRATION_DOES_NOT_FIT_ITS_BASE`: the top counts are joined on the key, and in SQL `NULL = NULL` is not true, so that row finds no top counts. The other gates check the arithmetic of the notebook itself.

### The audit row

| Column | Content for G3 |
|---|---|
| `source_rows_read` | Six keys, one per Silver table: `push_events`, `pull_request_events`, `issue_events`, `issue_comment_events`, `pull_request_review_events`, `pull_request_review_comment_events`. Each is the number of Silver rows on the affected days |
| `rows_excluded` | Key `missing_event_date`: rows in the scope of the run without an event date, the six tables together |
| `quality_metrics` | Nine numbers over the rows of this run: `repository_days`, the sums of `engineering_activity_events`, `attributable_events`, `unattributable_events`, `unique_active_actors`, `top_actor_events` and `top_3_actor_events`, and two row counts: `rows_without_attributable_events` (rows whose concentration values are NULL) and `single_actor_rows` (rows with exactly one person) |

After a run for all dates, these totals can be compared directly with the tables below. The sum of `unique_active_actors` counts person-repository-days: a person is counted once for every repository and day on which they worked.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 4,318,172 |
| Sum of `engineering_activity_events` | 31,757,399: the rows of the six Silver tables |
| Sum of `unattributable_events` | 0, so `actor_coverage_pct` is 100.00 on every row |
| `rows_without_attributable_events` | 0 |
| Sum of `unique_active_actors` | Not measured before the first run. At least 833,632, the people of G4, whose three tables are part of these six. At most 6,881,398, the people of G6 over all nine tables |
| Sums of the top counts, `single_actor_rows` | Not measured before the first run |

Events per day, for a run with one date. Each day is the sum of the six Silver tables on that day: the events of G6 on that day without its watch, fork and release events. The rows per day are the G3 rows of the profiling run in section 18. On every day, this table has at most as many rows as G6.

| Day | Engineering events |
|---|---|
| 2024-02-01 | 4,649,982 |
| 2024-02-02 | 4,666,392 |
| 2024-02-03 | 4,220,907 |
| 2024-02-04 | 4,183,626 |
| 2024-02-05 | 4,515,629 |
| 2024-02-06 | 4,721,651 |
| 2024-02-07 | 4,799,212 |
| **Week** | **31,757,399** |

### Results on the real week

The first two runs of G3 on the real data, on 22 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. 1 affected day, 4,220,907 Silver rows read in the six tables together, 495,687 rows published with `REPLACE WHERE` |
| All dates | `SUCCEEDED`, no gate failed. 7 affected days, 31,757,399 Silver rows read, 4,318,172 rows published with `INSERT OVERWRITE` |

- Every expected value of the tables above was met exactly: the rows, the events of the week and the events of every day. The rows of every day are the G3 rows of section 18.
- The totals of 3 February were the same after both runs.
- No row was without an event date, and no event ID appeared twice in the six tables together.
- The Silver rows read per table are the events of that kind in G6, for 3 February and for the week.
- Every event has a known actor, so the coverage is 100 on every row and no row has NULL in its concentration values.
- The sum of `unique_active_actors` is 5,282,093 person-repository-days: between the 833,632 of G4 and the 6,881,398 of G6, and below G6 on every day.
- 3,819,869 rows (88.46%) have exactly one person. They hold 25,249,572 events, 79.51% of the week.
- The most active person of each repository-day did 91.15% of the engineering events of the week, and the top 3 did 97.62%. On 3 and 4 February (a weekend) the most active person did about 95%, on the other days 89% to 91%.
- The repository-day with the most people is `llvm/llvm-project` on 6 February: 329 people, a top share of 9.01 and an HHI of 0.0205.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

| Day | Rows | Engineering events | People | Top actor events | Top 3 actor events | Rows with one person |
|---|---|---|---|---|---|---|
| 2024-02-01 | 677,026 | 4,649,982 | 846,260 | 4,140,709 | 4,509,942 | 591,023 |
| 2024-02-02 | 634,118 | 4,666,392 | 783,245 | 4,227,041 | 4,544,366 | 559,054 |
| 2024-02-03 | 495,687 | 4,220,907 | 566,247 | 4,027,966 | 4,185,640 | 454,349 |
| 2024-02-04 | 507,896 | 4,183,626 | 581,143 | 3,981,666 | 4,143,413 | 465,297 |
| 2024-02-05 | 665,685 | 4,515,629 | 833,529 | 4,035,125 | 4,384,686 | 579,184 |
| 2024-02-06 | 677,412 | 4,721,651 | 849,170 | 4,218,310 | 4,576,062 | 590,717 |
| 2024-02-07 | 660,348 | 4,799,212 | 822,499 | 4,315,407 | 4,657,221 | 580,245 |
| **Week** | **4,318,172** | **31,757,399** | **5,282,093** | **28,946,224** | **31,001,330** | **3,819,869** |

The rows of the week by their number of people. The two averages are averages of the rows.

| People | Rows | Engineering events | Average top share | Average HHI |
|---|---|---|---|---|
| 1 | 3,819,869 | 25,249,572 | 100.00 | 1.0000 |
| 2 | 328,767 | 2,615,459 | 66.67 | 0.5892 |
| 3 | 91,321 | 1,161,234 | 54.73 | 0.4417 |
| 4 to 10 | 70,423 | 1,794,623 | 45.23 | 0.3264 |
| More than 10 | 7,792 | 936,511 | 28.84 | 0.1621 |

### How to read it

- A repository with one active person on a day has a top share of 100 and an HHI of 1 by definition. Compare only repository-days with enough events and people. 59% of the rows are built from one or two events.
- Bots are actors in version 1. Logins that end in `[bot]` produce 17.5% of the engineering events of the week.
- The largest accounts are automation with an ordinary login. The single most active account produced 16.9% of all engineering events in only 30 repositories, and the 20 most active accounts together produced about 48%. A filter on `[bot]` would not remove them. Any list of "most active repositories" is led by automated repositories.
- One event is not one unit of work: a push with 100 commits is one event.
- Pushes are 83.6% of the input, so the result mostly reflects who pushes.
- The share of several rows together is calculated from the sums. It is not the average of the shares of the rows.

## 12. G4 `repository_collaboration_metrics`

**Question.** For each repository and day: how much reviewing and discussing happened, on what, and by how many people?

| | |
|---|---|
| Sources | Three Silver tables: `silver.issue_comment_events`, `silver.pull_request_review_events` and `silver.pull_request_review_comment_events`. |
| Grain | One row per repository per day with at least one event in the three tables. |
| Key | `repo_id`, `activity_date` |

### Three kinds of events

| Kind | Silver table | What one event is |
|---|---|---|
| Issue comment | `issue_comment_events` | A comment in the conversation of an issue or of a pull request |
| Review | `pull_request_review_events` | A review of a pull request, with a state such as `approved` |
| Inline review comment | `pull_request_review_comment_events` | A comment on a line of code in a pull request |

The three tables are placed under each other and counted as one list of events. They are never joined, because a join of raw events from two tables would multiply the rows.

### Rules

- One event is one row and is counted once. In this week every known action in the three tables is `created`, so edits and deletions do not appear and the action is not used. Two issue comment events have no action at all. They still count as events.
- Issue comments are split by `comment_target_type`: `issue`, `pull_request` or unknown. Unknown means that the value is missing. Unknown stays unknown and is never counted as an issue.
- Silver writes only these three targets. Any other value is in none of the three target columns, so the gate `TARGETS_DO_NOT_ADD_UP` fails and nothing is published.
- Review events are split by `review_state`: `approved`, `changes_requested`, `commented`, `dismissed`, or another known state such as `pending`. The state is compared exactly as Silver stores it. A state written in another way, for example in capital letters, counts as another known state.
- A review without a state is a review, but it is in none of the five state columns and not in `review_state_known_events`.
- People are counted once across all three tables: someone who comments and reviews in the same repository on the same day is one collaborator.
- An event without an actor is an event, but not a person. A repository-day whose events all have no actor has 0 people and a coverage of 0.
- Every row has at least one event, so the coverage is never NULL.
- The `repo_name` label is the name of the latest event with a name across the three tables, see section 5.

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
| `collaborator_actor_coverage_pct` | `DOUBLE` | 100 × actor-known ÷ total, rounded to two decimals |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

Added to the manual's proposal: `repo_name`, `review_dismissed_events`, `review_other_state_events`, `actor_known_events`.

### Quality gates on every run

- The key is unique and never NULL.
- No count column is NULL.
- The three target columns add up to `issue_comment_events`.
- The five state columns add up to `review_state_known_events`, which is ≤ `review_events`.
- The total equals the sum of the three event types.
- Unique actors ≤ actor-known events ≤ total, and a row with actor-known events has at least one actor.
- The coverage lies between 0 and 100.
- A row has a coverage exactly when it has events.
- No `event_id` appears twice in the stacked input. This check reads the stacked Silver rows of the affected days, because the Gold rows no longer carry event IDs.
- The sum of each of the three event columns equals the Silver row count of its table for the same days. The Silver rows are counted directly on the Silver tables.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `COUNT_IS_NULL`, `TARGETS_DO_NOT_ADD_UP`, `STATES_DO_NOT_ADD_UP`, `KNOWN_STATES_ABOVE_REVIEWS`, `TOTAL_IS_NOT_THE_SUM`, `ACTOR_COUNTS_OUT_OF_ORDER`, `COVERAGE_OUT_OF_RANGE`, `COVERAGE_DOES_NOT_FIT_ITS_BASE`, `EVENT_ID_NOT_UNIQUE`, `EVENT_SUM_DIFFERS_FROM_SILVER`.

`EVENT_SUM_DIFFERS_FROM_SILVER` is one name for three comparisons, one per Silver table. The printed output of the notebook shows the three pairs of numbers.

### The audit row

| Column | Content for G4 |
|---|---|
| `source_rows_read` | Three keys, one per Silver table: `issue_comment_events`, `pull_request_review_events`, `pull_request_review_comment_events`. Each is the number of Silver rows on the affected days |
| `rows_excluded` | Key `missing_event_date`: rows in the scope of the run without an event date, the three tables together |
| `quality_metrics` | `repository_days` and the sums of the fifteen count columns over the rows of this run: `issue_comment_events`, `issue_conversation_comment_events`, `pr_conversation_comment_events`, `unclassified_conversation_comment_events`, `review_events`, `review_state_known_events`, `review_approved_events`, `review_changes_requested_events`, `review_commented_events`, `review_dismissed_events`, `review_other_state_events`, `inline_review_comment_events`, `collaboration_events_total`, `actor_known_events`, `unique_collaborating_actors` |

After a run for all dates, these totals can be compared directly with the tables below. The sum of `unique_collaborating_actors` counts person-repository-days: a person is counted once for every repository and day on which they took part.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 444,720 |
| Sum of `collaboration_events_total` | 2,497,518 |
| Sum of `issue_comment_events` | 1,357,324 |
| Sum of `review_events` | 717,277 |
| Sum of `inline_review_comment_events` | 422,917 |
| Sum of `unclassified_conversation_comment_events` | 2 |
| Sum of `review_state_known_events` | 717,277, because no review has a missing state |
| Sum of `actor_known_events` | 2,497,518, because no Silver row has a missing actor. The coverage is 100 on every row |
| Sum of `unique_collaborating_actors` | Not measured before the first run. It cannot be above 2,497,518 |

The split by target and by state was measured only on the raw events, before repeated events were removed. Silver has 3 issue comment events and 8 review events fewer than the raw events, so each Gold sum should be at most that much lower:

| Value | Raw events |
|---|---|
| Comments in pull request conversations | 838,179 |
| Comments on issues | 519,146 |
| Reviews: approved | 339,097 |
| Reviews: commented | 323,312 |
| Reviews: changes requested | 43,110 |
| Reviews: dismissed | 11,744 |
| Reviews: another state (`pending`) | 22 |

Rows and events per day, for a run with one date:

| Day | Rows | Collaboration events |
|---|---|---|
| 2024-02-01 | 75,896 | 449,194 |
| 2024-02-02 | 67,275 | 393,209 |
| 2024-02-03 | 37,985 | 171,153 |
| 2024-02-04 | 37,587 | 174,277 |
| 2024-02-05 | 82,181 | 438,816 |
| 2024-02-06 | 73,223 | 444,097 |
| 2024-02-07 | 70,573 | 426,772 |
| **Week** | **444,720** | **2,497,518** |

### Results on the real week

The first two runs of G4 on the real data, on 17 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. 1 affected day, 171,153 Silver rows read (108,111 issue comment, 38,580 review and 24,462 inline review comment rows), 37,985 rows published with `REPLACE WHERE` |
| All dates | `SUCCEEDED`, no gate failed. 7 affected days, 2,497,518 Silver rows read (1,357,324 issue comment, 717,277 review and 422,917 inline review comment rows), 444,720 rows published with `INSERT OVERWRITE` |

- Every expected sum above was met exactly, and so were the rows and the events of every day.
- The totals of 3 February were the same after both runs.
- No row was without an event date, and no event ID appeared twice in the three tables together.
- By target: 519,146 comments on issues, 838,176 in pull request conversations and 2 with an unknown target. The 2 are on 2 February and on 6 February.
- By state: 339,096 approved, 43,110 changes requested, 323,305 commented, 11,744 dismissed and 22 in another state.
- Compared with the raw events, the pull request conversations are 3 lower, approved is 1 lower and commented is 7 lower. The other four values are equal. These are exactly the 3 issue comment events and the 8 review events that Silver removed as repeated.
- Every event has a known actor, so the coverage of the whole week is 100.
- The sum of `unique_collaborating_actors` is 833,632 person-repository-days.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

| Day | Rows | Collaboration events | Issue comments | On issues | In pull request conversations | Reviews | Approved | Changes requested | Commented | Dismissed | Other state | Inline review comments | Actors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2024-02-01 | 75,896 | 449,194 | 241,515 | 84,495 | 157,020 | 131,303 | 63,627 | 7,224 | 58,312 | 2,140 | 0 | 76,376 | 145,736 |
| 2024-02-02 | 67,275 | 393,209 | 206,277 | 80,152 | 126,124 | 116,124 | 53,795 | 6,927 | 53,358 | 2,042 | 2 | 70,808 | 129,398 |
| 2024-02-03 | 37,985 | 171,153 | 108,111 | 48,937 | 59,174 | 38,580 | 17,472 | 2,678 | 17,841 | 589 | 0 | 24,462 | 62,789 |
| 2024-02-04 | 37,587 | 174,277 | 110,017 | 51,427 | 58,590 | 40,604 | 18,457 | 3,014 | 18,537 | 596 | 0 | 23,656 | 63,310 |
| 2024-02-05 | 82,181 | 438,816 | 240,696 | 82,254 | 158,442 | 127,442 | 63,582 | 7,475 | 54,162 | 2,213 | 10 | 70,678 | 150,270 |
| 2024-02-06 | 73,223 | 444,097 | 232,232 | 88,238 | 143,993 | 133,168 | 62,339 | 8,101 | 60,531 | 2,190 | 7 | 78,697 | 143,805 |
| 2024-02-07 | 70,573 | 426,772 | 218,476 | 83,643 | 134,833 | 130,056 | 59,824 | 7,691 | 60,564 | 1,974 | 3 | 78,240 | 138,324 |
| **Week** | **444,720** | **2,497,518** | **1,357,324** | **519,146** | **838,176** | **717,277** | **339,096** | **43,110** | **323,305** | **11,744** | **22** | **422,917** | **833,632** |

In the table, the issue comments with an unknown target (2) and the reviews with a known state (equal to the reviews on every day) are left out.

### How to read it

- The numbers count events. An approving review event is not the same as "the pull request was approved".
- More comments do not mean better collaboration.
- Most issue comment events are pull request conversations, about 62% in this week.
- 38% of the issue comment events come from logins that end in `[bot]`. `unique_collaborating_actors` counts accounts, and an account can be a bot.
- 61% of the rows are built from one or two events. The count columns allow a reader to set a threshold.
- The coverage of several rows together is calculated from the two sums. It is not the average of the coverages of the rows.

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
- **Rule 7 is checked only for the earliest event of a release.** The other events of the release already have their reason, `REPEATED_RELEASE_EVENT`. So when the windows of the earliest event are not covered, the release has no anchor: that event counts as `WINDOW_NOT_COVERED` and the later ones as repeated events, even if a later one would have fitted. The profiling run applied the rules in the same order.
- An hour that is not in the manifest at all, for example an hour of 31 January, is not complete.
- A release tag is a label. The week has 158,588 releases but only 140,971 different tags per repository, so tags are reused.

### Window rules

- r is `release_published_at`. It is never replaced by the event time or the ingestion time.
- The before window runs from r − 24 hours up to, but not including, r. The after window runs from r up to, but not including, r + 24 hours.
- An event exactly at r counts in the after window. An event exactly at r + 24 hours is outside both windows.
- Rule 7 needs 48 complete hourly files when r is exactly on the hour, otherwise 49. The window touches every hour from the hour in which r − 24 hours lies up to the last hour that starts before r + 24 hours. Example: r = 4 February 12:30 touches the hours 3 February 12:00 to 5 February 12:00, 49 hours.
- Activity is matched by `repo_id` and `event_timestamp`. A row without an event time cannot be placed. It is left out and counted in `rows_excluded` under the key `missing_event_timestamp`.
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

- The key is unique and never NULL.
- The anchor values are never NULL: `release_id`, `release_published_at`, `is_prerelease`, `anchor_event_id` and the two window columns.
- `release_anchor_key` is the `release_id` written as text.
- The row count equals the number of anchors.
- Anchors plus all failure reasons equal the number of release events. The release events are counted directly on the Silver table.
- No count column is NULL.
- Before and after totals equal the sum of their five type columns.
- Change = after − before. The percent change is 100 × change ÷ before, rounded to 2 decimals, and NULL exactly when before is 0. The baseline flag is true exactly when before is 0.
- `window_start_at` is r − 24 hours and `window_end_at` is r + 24 hours.
- Every window lies inside complete hours. This gate asks the manifest again, from the window columns of the staged rows.
- Commit-eligible push events ≤ push events, the known commits are never negative, and a window without an eligible push has 0 known commits.
- No `event_id` appears twice in the six Silver tables together.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `ANCHOR_VALUE_IS_NULL`, `ANCHOR_KEY_DIFFERS_FROM_RELEASE_ID`, `ROWS_DIFFER_FROM_ANCHORS`, `REASONS_DO_NOT_ADD_UP`, `COUNT_IS_NULL`, `TOTALS_DO_NOT_ADD_UP`, `CHANGE_DOES_NOT_FIT`, `WINDOW_DOES_NOT_FIT_ITS_RELEASE`, `WINDOW_NOT_COMPLETE`, `PUSH_COMMITS_OUT_OF_ORDER`, `EVENT_ID_NOT_UNIQUE`.

Two gates can be set off by a Silver row that Silver itself would never write: `KEY_IS_NULL` (a valid release without `repo_id`) and `EVENT_ID_NOT_UNIQUE` (one event in two tables). The other gates check the arithmetic of the notebook itself.

No gate compares the sums of the activity counts with Silver. The windows of releases that are close together overlap, so one event can be counted for several rows, and most events lie in no window at all.

### The audit row

| Column | Content for G5 |
|---|---|
| `processing_scope` | Always `ALL_DATES` |
| `source_date` | The parameter, or NULL. It does not limit the run |
| `affected_dates` | The event dates of the release events |
| `source_rows_read` | Six keys, one per Silver table: `release_events`, `push_events`, `pull_request_events`, `issue_events`, `watch_events`, `fork_events`. Each is the number of rows of that table |
| `rows_excluded` | Key `missing_event_timestamp`: activity rows without an event time, the five tables together |
| `quality_metrics` | 14 numbers: `release_events`, `anchors`, the seven reasons under their own names (`NOT_PUBLISHED` to `WINDOW_NOT_COVERED`), `prerelease_anchors`, `baseline_zero_rows`, `rows_without_activity` (0 before and 0 after), `manifest_hours` and `complete_hours` |

No sum of the activity counts is written into the audit row, for the same reason as above.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 108,295, when every hour is complete in the manifest |
| Rows with `is_prerelease` true | 13,586 |
| Rows + all failure reasons | 159,416 |
| Failure reasons | The column "Expected" of the anchor rules above |
| `manifest_hours`, `complete_hours` | 168 and 168, as measured by the manifest run of 15 November 2026 |
| Rows without an event time | 0 |
| `baseline_zero_rows`, `rows_without_activity` | Not measured before the first run. The first run measured 26,136 and 21,228 |
| The activity counts | Not measured before the first run, and not in the audit row |

### Results on the real week

The first two runs of G5 on the real data, on 22 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. Scope `ALL_DATES`, 159,416 release events checked, 108,295 rows published with `INSERT OVERWRITE` |
| Blank `source_date` | `SUCCEEDED`, no gate failed. The same 108,295 rows, published with `INSERT OVERWRITE` |

- Every expected value of the table above was met exactly: 108,295 anchors, 13,586 prereleases, 278 `DRAFT_OR_UNKNOWN`, 740 `REPEATED_RELEASE_EVENT`, 50,103 `WINDOW_NOT_COVERED`, 0 for the other four reasons, and 159,416 release events together.
- The manifest had 168 hours, all complete.
- Both runs gave the same table: the same reasons, the same rows per day and the same ten rows of the sample. Only the run ID differed, so the source date changed nothing, as section 6 says.
- The Silver rows read per table are the row counts of section 1.
- No activity row was without an event time, and no event ID appeared twice in the six tables together.
- The anchors belong to 45,475 repositories.
- 26,136 rows (24.13%) have no activity before the release, so their percent change is empty. 21,228 of them have no activity after it either.
- 56,145 rows (51.84%) have less activity in the 24 hours after the release than before it, 24,225 (22.37%) have more, and 27,925 (25.79%) have the same, most of them because they have no activity at all.
- The first anchor was published on 2 February at 00:00:02 UTC. The last was published on 7 February at exactly 00:00:00 UTC, so its after window ends exactly at the end of the week. It is the only anchor of 7 February.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

| Day of the release | Rows | Prereleases | Baseline zero | More after | Less after |
|---|---|---|---|---|---|
| 2024-02-02 | 24,012 | 3,084 | 5,344 | 4,208 | 13,926 |
| 2024-02-03 | 18,727 | 2,071 | 5,199 | 4,057 | 9,203 |
| 2024-02-04 | 18,061 | 2,328 | 4,902 | 4,108 | 8,764 |
| 2024-02-05 | 23,366 | 3,036 | 5,347 | 6,409 | 11,334 |
| 2024-02-06 | 24,128 | 3,067 | 5,344 | 5,443 | 12,917 |
| 2024-02-07 | 1 | 0 | 0 | 0 | 1 |
| **Week** | **108,295** | **13,586** | **26,136** | **24,225** | **56,145** |

The day of the release is the UTC date of `release_published_at`. 1 February has no row, because no release of that day has 24 hours of the week before it.

### How to read it

- It is a before-and-after comparison, not proof that the release caused the change.
- Windows of releases that are close together overlap. Do not add rows up across releases.
- The day of the week and the time of day also change activity.
- Releases from the first 24 hours of the week and releases after 7 February 00:00 UTC are not in the table. A release at exactly 7 February 00:00:00 UTC just fits, and the real week has one.
- One source hour is thin: on 5 February 2024 from about 19:24 to 20:00 UTC, Bronze holds far fewer events than usual (section 8). A release published in the 24 hours before these minutes has them in its after window, so its after count is slightly low and the change looks more negative than it was. A release published in the 24 hours after them, until 6 February 20:00 UTC, has them in its before window, so its before count is slightly low and the change looks more positive than it was. Over all event types and repositories, about 2% of the events of such a 24-hour window are missing. That is an estimate, and for one repository it can be more or less.
- A release often comes with its own pushes, which count as activity.
- Many releases are published by automation. 7,416 of the 62,125 repositories with releases have no activity event in the whole week, so many rows show 0 before and 0 after, an empty percentage and `baseline_zero_flag` true. That is a real result.

## 14. G6 `repository_activity_profile`

**Question.** For each repository and day: how much activity was there, and how is it split over the nine event types?

| | |
|---|---|
| Sources | All nine Silver tables. |
| Grain | One row per repository per day with at least one event in the nine tables. |
| Key | `repo_id`, `activity_date` |

The notebook `06_repository_activity_profile` builds all columns of this section. The core columns came first. The optional columns came in a second step, with their own pull request, and the notebook adds them to a Gold table that the first version created.

### Nine kinds of events

| Kind | Silver table | Gold column |
|---|---|---|
| Push | `push_events` | `push_events` |
| Pull request | `pull_request_events` | `pull_request_events` |
| Issue | `issue_events` | `issue_events` |
| Issue comment | `issue_comment_events` | `issue_comment_events` |
| Review | `pull_request_review_events` | `review_events` |
| Inline review comment | `pull_request_review_comment_events` | `inline_review_comment_events` |
| Watch (a new star) | `watch_events` | `watch_events` |
| Fork | `fork_events` | `fork_events` |
| Release | `release_events` | `release_events` |

The nine tables are placed under each other and counted as one list of events. They are never joined, because a join of raw events from two tables would multiply the rows.

### Rules

- One event is one row and is counted once. Each event belongs to exactly one kind: the kind of the Silver table it comes from. The nine counts therefore add up to the total.
- A kind without events has the count 0 and the share 0. That is a measured zero, never NULL.
- A share is 100 × the count of the kind ÷ the total, rounded to two decimals. The total is never 0, because a row exists only when there is at least one event.
- Each share is rounded on its own, so the nine shares of a row add up to 100 only within rounding. Nine shares can be off by up to 0.045 together, so the gate allows 99.95 to 100.05. Example: 90 events split 40, 15, 12, 8, 4, 3, 5, 2 and 1 give shares that add up to 99.99.
- A share of 0.00 next to a count above 0 is correct, and so is a share of 100.00 next to events of other kinds: 1 watch event among 25,001 events has the share 0.00, and the 25,000 pushes have 100.00. The count columns hold the exact values.
- People are counted once across all nine tables: someone who pushes, comments and stars in the same repository on the same day is one actor.
- An event without an actor is an event, but not a person. A repository-day whose events all have no actor has 0 people and a coverage of 0.
- The `repo_name` label is the name of the latest event with a name across the nine tables, see section 5.
- Broader groups, such as "review" for reviews plus inline comments, are not stored. A report can add them from the nine counts.
- A push is one event, however many commits it has. Commit numbers are a separate measure and are never added to the event counts.

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
| `push_share_pct`, `pull_request_share_pct`, `issue_share_pct`, `issue_comment_share_pct`, `review_share_pct`, `inline_review_comment_share_pct`, `watch_share_pct`, `fork_share_pct`, `release_share_pct` | `DOUBLE` | One share per type: 100 × count ÷ total, rounded to two decimals |
| `actor_known_events` | `BIGINT` | Events with a known actor |
| `unique_known_actors` | `BIGINT` | Different actors across all nine types |
| `actor_attribution_coverage_pct` | `DOUBLE` | 100 × actor-known ÷ total, rounded to two decimals |
| `_gold_run_id`, `_gold_updated_at` | | Technical columns, see section 5 |

### Optional columns

They are placed after the core columns and before the two technical columns. They were added in the second part of step 8.

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

- **Active hours** count the different hours of `event_timestamp` in UTC, over the nine kinds together. Ten events between 14:00 and 14:59 are one active hour. A row has at least one event, so it has 1 to 24 active hours.
- **Push commits.** Silver leaves a commit count NULL when it cannot be trusted. Only pushes with a known count are added up, and the eligible column says how many pushes that is. When no push of a repository-day has a known count, the sum is 0 and the eligible column is 0 too. A day without pushes has 0 in all four push columns. None of them is NULL.
- In this week 611 push events have no usable commit counts: they had more distinct commits than commits, and Silver set both counts to NULL. Every other push has both counts.
- **The comment split** is the one of G4, section 12: `comment_target_type` is `issue`, `pull_request` or unknown (NULL). Unknown stays unknown. The three columns add up to `issue_comment_events`, and any other target value fails the gate `TARGETS_DO_NOT_ADD_UP`.
- The push commits and the comment split are counted on the push table and the issue comment table directly, per repository and day, and joined to the counts on the key. A repository-day without pushes or without issue comments has 0 in those columns.

**A Gold table of the first version.** `CREATE TABLE IF NOT EXISTS` does not change a table that exists. So the notebook checks whether the table has the column `active_utc_hours`. If not, it adds the eight columns with `ALTER TABLE ... ADD COLUMNS`, each one `AFTER` the one before it, starting after `actor_attribution_coverage_pct`. The rows already in the table keep NULL in the new columns until a run rebuilds their day. After the change, one run for all dates fills every row.

### Quality gates on every run

- The key is unique and never NULL.
- No count column is NULL, and no share is NULL.
- The nine counts add up to the total.
- Each share lies between 0 and 100, and a kind without events has the share 0.
- The nine shares add up to 100, within rounding (99.95 to 100.05).
- Unique actors ≤ actor-known events ≤ total, and a row with actor-known events has at least one actor.
- The coverage lies between 0 and 100.
- A row has a coverage exactly when it has events.
- No `event_id` appears twice in the stacked input. This check reads the stacked Silver rows of the affected days, because the Gold rows no longer carry event IDs.
- The sum of each of the nine event columns equals the Silver row count of its table for the same days. The Silver rows are counted directly on the Silver tables.
- The active hours lie between 1 and 24 and are not above the number of events.
- Pushes with a known count ≤ push events, for both counts. A commit sum is never below 0, and it is 0 when no push has a known count.
- Comments on issues + in pull request conversations + with an unknown target = issue comment events.

The names of the gates, as they appear in `failed_checks`: `KEY_NOT_UNIQUE`, `KEY_IS_NULL`, `COUNT_IS_NULL`, `TYPES_DO_NOT_ADD_UP`, `SHARE_IS_NULL`, `SHARE_OUT_OF_RANGE`, `SHARE_WITHOUT_EVENTS`, `SHARES_DO_NOT_ADD_UP`, `ACTOR_COUNTS_OUT_OF_ORDER`, `COVERAGE_OUT_OF_RANGE`, `COVERAGE_DOES_NOT_FIT_ITS_BASE`, `ACTIVE_HOURS_OUT_OF_RANGE`, `PUSH_COMMITS_OUT_OF_ORDER`, `TARGETS_DO_NOT_ADD_UP`, `EVENT_ID_NOT_UNIQUE`, `EVENT_SUM_DIFFERS_FROM_SILVER`. `COUNT_IS_NULL` covers the twenty count columns, the eight optional ones included.

`EVENT_SUM_DIFFERS_FROM_SILVER` is one name for nine comparisons, one per Silver table. The printed output of the notebook shows the nine pairs of numbers.

Five gates can be set off by a Silver row that Silver itself would never write: `KEY_IS_NULL` (a row without `repo_id`), `EVENT_ID_NOT_UNIQUE` (one event in two tables), `ACTIVE_HOURS_OUT_OF_RANGE` (an event date without an event time), `PUSH_COMMITS_OUT_OF_ORDER` (a negative commit count) and `TARGETS_DO_NOT_ADD_UP` (a target other than `issue`, `pull_request` or NULL). The kind of an event is simply the table it comes from, so the other gates check the arithmetic of the notebook itself.

### The audit row

| Column | Content for G6 |
|---|---|
| `source_rows_read` | Nine keys, one per Silver table: `push_events`, `pull_request_events`, `issue_events`, `issue_comment_events`, `pull_request_review_events`, `pull_request_review_comment_events`, `watch_events`, `fork_events`, `release_events`. Each is the number of Silver rows on the affected days |
| `rows_excluded` | Key `missing_event_date`: rows in the scope of the run without an event date, the nine tables together |
| `quality_metrics` | `repository_days` and the sums of the twelve count columns over the rows of this run: `all_retained_event_count`, `push_events`, `pull_request_events`, `issue_events`, `issue_comment_events`, `review_events`, `inline_review_comment_events`, `watch_events`, `fork_events`, `release_events`, `actor_known_events`, `unique_known_actors`, and the sums of the eight optional columns: `active_utc_hours`, `push_commit_eligible_events`, `known_push_commits`, `push_distinct_commit_eligible_events`, `known_push_distinct_commits`, `issue_conversation_comment_events`, `pr_conversation_comment_events`, `unclassified_conversation_comment_events` |

After a run for all dates, these totals can be compared directly with the tables below. The sum of `unique_known_actors` counts person-repository-days: a person is counted once for every repository and day on which they took part. In the same way, the sum of `active_utc_hours` counts repository-hours.

### Expected on the real week

| Value | Expected |
|---|---|
| Rows | 5,000,573 |
| Sum of `all_retained_event_count` | 33,623,791 |
| Sum of `push_events` | 26,536,967 |
| Sum of `pull_request_events` | 2,173,958 |
| Sum of `issue_events` | 548,956 |
| Sum of `issue_comment_events` | 1,357,324 |
| Sum of `review_events` | 717,277 |
| Sum of `inline_review_comment_events` | 422,917 |
| Sum of `watch_events` | 1,380,559 |
| Sum of `fork_events` | 326,417 |
| Sum of `release_events` | 159,416 |
| Sum of `actor_known_events` | 33,623,791, because no Silver row has a missing actor. The coverage is 100 on every row |
| Sum of `unique_known_actors` | Not measured before the first run. It cannot be above 33,623,791. The first run measured 6,881,398 |
| Sum of `push_commit_eligible_events` | 26,536,356: all pushes except the 611 without usable counts |
| Sum of `push_distinct_commit_eligible_events` | 26,536,356, the same pushes |
| Sums of `known_push_commits` and `known_push_distinct_commits` | Not measured before the first run. The first run measured 81,686,969 and 48,608,061 |
| Sum of `issue_conversation_comment_events` | 519,146, as in G4 |
| Sum of `pr_conversation_comment_events` | 838,176, as in G4 |
| Sum of `unclassified_conversation_comment_events` | 2, as in G4 |
| Sum of `active_utc_hours` | Not measured before the first run. At least 5,000,573 (one per row) and not above 33,623,791 (one per event). The first run measured 10,144,599 |
| Rows with NULL in an optional column | 0 after a run for all dates |

Each type sum is the row count of its Silver table in section 1.

Rows and events per day, for a run with one date:

| Day | Rows | All events |
|---|---|---|
| 2024-02-01 | 780,577 | 4,931,231 |
| 2024-02-02 | 735,149 | 4,944,140 |
| 2024-02-03 | 579,908 | 4,447,005 |
| 2024-02-04 | 602,850 | 4,441,124 |
| 2024-02-05 | 763,841 | 4,789,485 |
| 2024-02-06 | 777,905 | 4,999,343 |
| 2024-02-07 | 760,343 | 5,071,463 |
| **Week** | **5,000,573** | **33,623,791** |

The events of each day by kind. Pull requests, issues and the three collaboration kinds are the results of G1, G2 and G4 in Databricks. Pushes and "watch + fork + release" are worked out from those results and the profiling numbers of G3 and G6, and they add up to the Silver row counts of section 1:

| Day | Push | Pull request | Issue | Issue comment + review + inline | Watch + fork + release |
|---|---|---|---|---|---|
| 2024-02-01 | 3,723,327 | 387,295 | 90,166 | 449,194 | 281,249 |
| 2024-02-02 | 3,858,570 | 332,523 | 82,090 | 393,209 | 277,748 |
| 2024-02-03 | 3,793,533 | 197,802 | 58,419 | 171,153 | 226,098 |
| 2024-02-04 | 3,761,468 | 189,714 | 58,167 | 174,277 | 257,498 |
| 2024-02-05 | 3,604,406 | 388,940 | 83,467 | 438,816 | 273,856 |
| 2024-02-06 | 3,843,012 | 345,716 | 88,826 | 444,097 | 277,692 |
| 2024-02-07 | 3,952,651 | 331,968 | 87,821 | 426,772 | 272,251 |
| **Week** | **26,536,967** | **2,173,958** | **548,956** | **2,497,518** | **1,866,392** |

The comment split of each day is the one G4 measured in Databricks:

| Day | Comments on issues | In pull request conversations | Unknown target |
|---|---|---|---|
| 2024-02-01 | 84,495 | 157,020 | 0 |
| 2024-02-02 | 80,152 | 126,124 | 1 |
| 2024-02-03 | 48,937 | 59,174 | 0 |
| 2024-02-04 | 51,427 | 58,590 | 0 |
| 2024-02-05 | 82,254 | 158,442 | 0 |
| 2024-02-06 | 88,238 | 143,993 | 1 |
| 2024-02-07 | 83,643 | 134,833 | 0 |
| **Week** | **519,146** | **838,176** | **2** |

The 611 pushes without usable counts were not measured per day, so the pushes with a known count are given for the week only.

### Results on the real week

The first two runs of G6 on the real data, on 17 November 2026 (UTC). They ran the first version, with the core columns only:

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, no gate failed. 1 affected day, 4,447,005 Silver rows read in the nine tables together, 579,908 rows published with `REPLACE WHERE` |
| All dates | `SUCCEEDED`, no gate failed. 7 affected days, 33,623,791 Silver rows read, 5,000,573 rows published with `INSERT OVERWRITE` |

- Every expected value of the tables above was met exactly: the rows and the events of every day, and the events of every day by kind.
- The totals of 3 February were the same after both runs.
- No row was without an event date, and no event ID appeared twice in the nine tables together.
- On every day, `pull_request_events` equals the pull request events of G1, `issue_events` the issue events of G2, and issue comments + reviews + inline review comments the events of G4.
- Every event has a known actor, so the coverage is 100 on every row. The sum of `unique_known_actors` is 6,881,398 person-repository-days.
- Pushes are 78.92% of all events of the week.
- The rounding rule shows on real rows: on 3 February one repository has 63,650 pushes and 1 issue event. Its push share is 100.00 and its issue share is 0.00.
- Each publish was one Delta commit: the Gold table was at version 1 after the first run and at version 2 after the second.

The kinds not shown in the tables above, and the people:

| Day | Rows | All events | Push | Watch | Fork | Release | Actors |
|---|---|---|---|---|---|---|---|
| 2024-02-01 | 780,577 | 4,931,231 | 3,723,327 | 205,001 | 50,016 | 26,232 | 1,086,727 |
| 2024-02-02 | 735,149 | 4,944,140 | 3,858,570 | 206,010 | 47,587 | 24,151 | 1,021,146 |
| 2024-02-03 | 579,908 | 4,447,005 | 3,793,533 | 168,900 | 38,301 | 18,897 | 759,547 |
| 2024-02-04 | 602,850 | 4,441,124 | 3,761,468 | 197,635 | 41,636 | 18,227 | 805,632 |
| 2024-02-05 | 763,841 | 4,789,485 | 3,604,406 | 202,311 | 48,037 | 23,508 | 1,068,809 |
| 2024-02-06 | 777,905 | 4,999,343 | 3,843,012 | 202,413 | 50,999 | 24,280 | 1,085,320 |
| 2024-02-07 | 760,343 | 5,071,463 | 3,952,651 | 198,289 | 49,841 | 24,121 | 1,054,217 |
| **Week** | **5,000,573** | **33,623,791** | **26,536,967** | **1,380,559** | **326,417** | **159,416** | **6,881,398** |

The two runs of the second version, with the optional columns, on 17 November 2026 (UTC). Before them, the Gold table held the rows of the first version:

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | Step 2.1 added the eight optional columns: 35 columns. `SUCCEEDED`, no gate failed. 1 affected day, 4,447,005 Silver rows read, 579,908 rows published with `REPLACE WHERE`. After the run, the rows of 3 February had the optional values, and the rows of the six other days still had NULL in them |
| All dates | Step 2.1 found the columns and changed nothing. `SUCCEEDED`, no gate failed. 7 affected days, 33,623,791 Silver rows read, 5,000,573 rows published with `INSERT OVERWRITE`. No row has NULL in an optional column |

- Every expected value was met exactly: the 611 pushes without usable counts, the comment split of every day (equal to G4), and the core columns of every day (equal to the first version).
- The values of 3 February were the same after both runs.
- The test notebook ran before the two runs. The production audit, Gold and staging tables had the same Delta version before and after it.
- The Gold table was at version 4 after the first run (the `ADD COLUMNS` is version 3) and at version 6 after the second. That is one version more than the notebook writes; the cause was not checked. A test must never expect an exact Delta version.

The optional values per day:

| Day | Active hours | Pushes with a known commit count | Pushes without | Their commits | Commits per push |
|---|---|---|---|---|---|
| 2024-02-01 | 1,577,346 | 3,723,216 | 111 | 11,258,748 | 3.02 |
| 2024-02-02 | 1,485,271 | 3,858,476 | 94 | 10,924,538 | 2.83 |
| 2024-02-03 | 1,201,741 | 3,793,451 | 82 | 18,770,772 | 4.95 |
| 2024-02-04 | 1,247,168 | 3,761,401 | 67 | 8,227,864 | 2.19 |
| 2024-02-05 | 1,522,952 | 3,604,318 | 88 | 10,357,934 | 2.87 |
| 2024-02-06 | 1,567,736 | 3,842,920 | 92 | 10,985,000 | 2.86 |
| 2024-02-07 | 1,542,385 | 3,952,574 | 77 | 11,162,113 | 2.82 |
| **Week** | **10,144,599** | **26,536,356** | **611** | **81,686,969** | **3.08** |

- The pushes with a known distinct commit count are the same pushes. Their distinct commits add up to 48,608,061 for the week, 1.83 per push.
- A repository-day has 2.03 active hours on average.
- 3 February has about 5 commits per push, the other days 2.2 to 3.0. The cause was not checked.
- The ten repository-days with the most events are pushes of automated repositories, active in all 24 hours, with about one commit per push.

### How to read it

- The table shows a mix of event counts. It does not measure the amount of work or the health of a project.
- Watch events are new stars only. Removed stars are not in the data.
- Pushes are 78.9% of all events, so most repository-days are mostly pushes.
- 63% of the rows are built from one or two events. The count columns allow a reader to set a threshold.
- The repositories with the most events are automated. One repository has 64,783 events on a single day.
- The share of several rows together is calculated from the sums. It is not the average of the shares of the rows.
- The active hours say how spread out the activity of a day is. They do not say how long anybody worked. On 5 February most events from about 19:24 to 20:00 UTC are missing (section 8), so a few rows of that day may miss an active hour.
- The commit columns count only the pushes with a known count. For commits per push, divide by the eligible column, not by `push_events`.

## 15. The final Gold audit

`09_gold_final_audit` runs after the six tables. It reads them, compares them with Silver and with each other, writes one audit row and fails its task if any check fails. It writes no Gold table.

| Check | Names in `failed_checks` | Expected on the full week |
|---|---|---|
| Every Gold table: key unique and never NULL | `G1_KEY_NOT_UNIQUE`, `G1_KEY_IS_NULL`, and the same for G2 to G6 | 0 violations |
| G1: rows and sum of `pr_event_count` | `G1_ROWS_DIFFER_FROM_SILVER`, `G1_EVENTS_DIFFER_FROM_SILVER` | 700,486 and 2,173,958 |
| G2: rows and sum of `issue_event_count` | `G2_ROWS_...`, `G2_EVENTS_...` | 194,627 and 548,956 |
| G3: rows and sum of `engineering_activity_events` | `G3_ROWS_...`, `G3_EVENTS_...` | 4,318,172 and 31,757,399 |
| G4: rows and sum of `collaboration_events_total` | `G4_ROWS_...`, `G4_EVENTS_...` | 444,720 and 2,497,518 |
| G6: rows and sum of `all_retained_event_count` | `G6_ROWS_...`, `G6_EVENTS_...` | 5,000,573 and 33,623,791 |
| G6: each of the nine kind columns = the rows of its Silver table | `G6_KIND_DIFFERS_FROM_SILVER` | The rows of section 1 |
| G5 rows + G5 failure reasons | `G5_DOES_NOT_ADD_UP_TO_SILVER` | 159,416, the number of release events |
| G6 `pull_request_events` = G1 `pr_event_count`, for every repository-day | `G6_PULL_REQUESTS_DIFFER_FROM_G1` | Equal |
| G6 `issue_events` = G2 `issue_event_count`, for every repository-day | `G6_ISSUES_DIFFER_FROM_G2` | Equal |
| G6 six engineering types = G3 total | `G6_ENGINEERING_DIFFERS_FROM_G3` | Equal |
| G6 issue comments + reviews + inline comments = G4 total | `G6_COLLABORATION_DIFFERS_FROM_G4` | Equal |
| G6 comment split = G4 comment split, for every repository-day | `G6_COMMENT_SPLIT_DIFFERS_FROM_G4` | Equal |
| Actors in G6 ≥ actors in G3 ≥ actors in G4, for every repository-day | `ACTORS_G6_BELOW_G3`, `ACTORS_G3_BELOW_G4` | True |
| Every repository-day of G1 to G4 exists in G6 | `G1_TO_G4_ROW_NOT_IN_G6` | True |
| Every G5 repository exists in G6 | `G5_REPOSITORY_NOT_IN_G6` | True |

These are 33 checks: 12 for the keys, 12 of Gold against Silver and 9 of Gold against Gold. They run in the order of the table, and `failed_checks` names the failed ones in that order. The fixed numbers above are written here for a person to compare with, they are not built into the code. The audit compares Gold with Silver and Gold with Gold.

### How the checks are made

- **Rows of a daily table.** G1, G2, G3, G4 and G6 have one row for every repository and day with at least one event of their kinds. So the rows of a table must be the number of different (`repo_id`, `event_date`) pairs in its Silver tables, and the sum of its event column must be the number of those Silver rows.
- **G5.** G5 checks every release event against seven rules (section 13), so its rows plus the seven reasons of its run add up to all release events. The audit takes the seven reasons from the newest `SUCCEEDED` audit row of `gold_release_activity_shift` for the same Gold table. When there is no such run, the check fails. When Silver got release events after that run, the sum is too small and the check fails too: G5 has to run again.
- **Gold against Gold.** A `FULL OUTER JOIN` on `repo_id` and `activity_date` keeps the rows of both tables, also a row without a partner, and a missing number counts as 0. A G6 row with 0 pull request events and no G1 row is therefore not a difference. The two checks of people use a `LEFT JOIN` from G3 to G6 and from G4 to G3, and a missing row counts as 0 people.
- **Every G5 repository exists in G6.** A release event is one of the nine kinds of G6, so a repository with an anchor has at least one G6 row on some day.

### All dates and one date

| | Blank `source_date` | A `source_date` |
|---|---|---|
| `processing_scope` | `ALL_DATES` | `SINGLE_DATE` |
| Days compared | The whole Gold tables against all Silver rows with an event date | The affected days of that source date, on both sides: the event dates of the Silver rows with that `_source_date`, in any of the nine tables |
| Always whole tables | The key checks, both G5 checks | The same |
| `NO_INPUT` | Silver has no row with an event date | No Silver row with that source date has an event date |

A run for all dates reads every Gold row, also a row whose day no longer exists in Silver. With one date, these are the days that G6 rebuilds for that date (section 6). Each other daily table rebuilds the part of them that has its own kinds. A Silver row without an event date cannot be placed on a day, so it is left out and counted under `missing_event_date`, as in the Gold notebooks.

With `NO_INPUT` no check runs. The run still writes its audit row and does not fail. On the real week, all seven days are compared, because every row has an event date and `event_date` always equals `_source_date` (section 18).

### The audit row

The final audit writes one row into `audit.gold_pipeline_runs` (section 7), with the 18 columns of every Gold run. Some of them mean something a little different, because it writes no table:

| Column | Value |
|---|---|
| `pipeline_name` | `gold_final_audit` |
| `gold_table` | NULL |
| `affected_dates` | The days that were compared |
| `source_rows_read` | The Silver rows on those days, one key per Silver table, with the table names of section 1 |
| `rows_excluded` | `missing_event_date`: Silver rows in the scope of the run without an event date |
| `gold_rows_published` | 0 |
| `gold_table_version` | NULL |
| `checks_passed`, `failed_checks` | As for the quality gates of a Gold notebook, with the check names of the table above |
| `quality_metrics` | `checks_run` and `checks_failed`; `g1_rows`, `g1_events` and the same for G2, G3, G4 and G6, on the days compared; `g5_rows`; `g5_left_out_release_events`, the seven reasons of the last successful G5 run, NULL when there is none; `silver_release_events_all_days` |

When a check fails, the audit row is written first, with the status `FAILED`. Then the notebook stops with an error, so the Job task fails too.

### Expected on the real week

A run with all widgets blank must give `SUCCEEDED` with these `quality_metrics`:

| Key | Expected |
|---|---|
| `checks_run`, `checks_failed` | 33 and 0 |
| `g1_rows`, `g1_events` | 700,486 and 2,173,958 |
| `g2_rows`, `g2_events` | 194,627 and 548,956 |
| `g3_rows`, `g3_events` | 4,318,172 and 31,757,399 |
| `g4_rows`, `g4_events` | 444,720 and 2,497,518 |
| `g6_rows`, `g6_events` | 5,000,573 and 33,623,791 |
| `g5_rows`, `g5_left_out_release_events` | 108,295 and 51,121 (278 + 740 + 50,103) |
| `silver_release_events_all_days` | 159,416 |

`source_rows_read` must hold the rows of section 1, `missing_event_date` must be 0, and `affected_dates` the seven days of the week. A run with one source date compares the rows of that day in section 18. For `source_date = 2024-02-03`: G1 68,242 rows and 197,802 events, G2 19,824 and 58,419, G3 495,687 and 4,220,907, G4 37,985 and 171,153, G6 579,908 and 4,447,005, and G5 as a whole as above.

### Results on the real week

The first two runs of the audit on the real data, on 23 November 2026 (UTC):

| Run | Result |
|---|---|
| `source_date = 2024-02-03` | `SUCCEEDED`, 33 of 33 checks passed. Scope `SINGLE_DATE`, one affected day (3 February), 4,447,005 Silver rows compared |
| Blank `source_date` | `SUCCEEDED`, 33 of 33 checks passed. Scope `ALL_DATES`, the seven days of the week, 33,623,791 Silver rows compared |

- Every expected value of the table above was met exactly, in both runs.
- In the blank run each daily table has as many rows as Silver has repository-days, and as many events as Silver has rows: G1 700,486 and 2,173,958, G2 194,627 and 548,956, G3 4,318,172 and 31,757,399, G4 444,720 and 2,497,518, G6 5,000,573 and 33,623,791.
- The nine kind columns of G6 equal their Silver tables. G5 has 108,295 rows, and its last successful run left out 51,121 release events. Together that is 159,416, all release events in Silver.
- No key appears twice or is NULL. G6 agrees with G1 to G4 on every repository-day, the people are in order (G6 ≥ G3 ≥ G4) everywhere, and every row of G1 to G4 and every G5 repository has a G6 row.
- No Silver row is without an event date, and the Silver rows read per table are the row counts of section 1.
- Each run took about 35 seconds and wrote one audit row, with no Gold table, 0 rows published and no table version.

### What `SUCCEEDED` does not prove

The audit proves that the six tables agree with Silver and with each other in their rows, counts and people. It does not prove that a value inside a row is right, for example a median, a share or a percent change. That is what the test notebook of each Gold table proves.

When a check fails, its line in step 7.1 of the notebook shows the number found and the number expected. The usual reason is a Gold table that was not rebuilt after Silver changed: run its Gold notebook again for the same source date, then the audit.

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

G5 is rebuilt completely, so "Other days untouched" and "A day that is gone" do not apply to it. Its test proves instead that a run for one source date, also a date without Silver rows, rebuilds the whole table, and that an hour that is not complete removes the releases whose windows touch it until the hour is complete again.

The test of the final audit builds the six Gold tables with the six real Gold notebooks in its test schema, and then runs the audit on them. It proves: no input; the exact numbers of hand-made rows for all dates and for one date, also a day before the source date that got a late row; that the audit writes no Gold table; that each of the 33 checks fails, with the number worked out by hand, on tables broken on purpose in one cell; recovery with `RESTORE TABLE`; that a late Silver row fails the audit until its Gold tables are rebuilt for its source date; bad parameters; and that no production table changed.

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
| 2026-11-16 | Step 6, G1. Section 9: a pull request number counts only inside its repository, a size of 0 is a known value, a repository-day without any pull request key still has a row, and an unknown action is in none of the four action columns. Three more quality gates (no count is NULL, the share lies between 0 and 100, a value exists exactly when its count is above 0), the names of the gates, the content of the audit row, and the expected rows and events per day. |
| 2026-11-16 | First runs of G1 in Databricks. Status and section 9: the test notebook passed (13 of 13 sections, 153 checks), and the runs for one date and for all dates met every expected value. Section 9 has the results per day and the measured number of snapshots. No rule changed. |
| 2026-11-17 | Step 7, G4. Section 12: the three kinds of events and their Silver tables, an unknown target is a missing value and any other value fails a gate, states are compared exactly as Silver stores them, a review without a state is in none of the state columns, a repository-day without any actor has 0 people and a coverage of 0. Three more quality gates (no count is NULL, the coverage lies between 0 and 100, a row has a coverage exactly when it has events) and one more condition in the actor gate (a row with actor-known events has at least one actor), the names of the gates, the content of the audit row, and the expected sums and the rows and events per day. |
| 2026-11-17 | First runs of G4 in Databricks. Status and section 12: the test notebook passed (13 of 13 sections, 158 checks), and the runs for one date and for all dates met every expected value. Section 12 has the results per day, the measured split by target and by state, and the measured number of person-repository-days. No rule changed. |
| 2026-11-17 | Step 8, G6, core columns. Section 14: the nine kinds and their Silver tables, each share is rounded on its own so the nine shares add up to 100 only within rounding, a share of 0.00 or 100.00 next to events of other kinds is correct, people and the label are taken across the nine tables, a repository-day without any actor has 0 people and a coverage of 0. More quality gates than planned (no count or share is NULL, each share lies between 0 and 100, a kind without events has the share 0, the coverage lies between 0 and 100 and exists exactly when the row has events, a row with actor-known events has at least one actor), the names of the gates, the content of the audit row, the expected sums and the events per day and kind. The optional columns follow in a second step. |
| 2026-11-17 | First runs of G6 in Databricks. Status and section 14: the test notebook passed (13 of 13 sections, 180 checks), and the runs for one date and for all dates met every expected value. Section 14 has the results per day and kind, and the measured number of person-repository-days. No rule changed. |
| 2026-11-17 | Step 8, G6, optional columns. Section 14: active hours in UTC over the nine kinds, push commits only over pushes with a known count (a sum over no known count is 0, next to an eligible count of 0), and the comment split of G4. A Gold table of the first version gets the eight columns with `ALTER TABLE ... ADD COLUMNS` at their place, and its old rows keep NULL in them until their day is rebuilt. Three more quality gates (`ACTIVE_HOURS_OUT_OF_RANGE`, `PUSH_COMMITS_OUT_OF_ORDER`, `TARGETS_DO_NOT_ADD_UP`), eight more quality numbers, and the expected values of the week. Section 15: the comment split of G6 and G4 is compared. |
| 2026-11-17 | First runs of the G6 optional columns in Databricks. Status and section 14: the test notebook passed (13 of 13 sections, 208 checks), the first run added the eight columns to the table of the first version, and the runs for one date and for all dates met every expected value. Section 14 has the optional values per day. No rule changed. |
| 2026-11-17 | Step 9, G3. Section 11: the notebook does not read the watch, fork and release tables at all, the numbers are counted in two levels (each person, then the repository-day), the people are ranked for each repository-day on its own with ties broken by `actor_id`, the HHI is calculated from the squared events of each person, a repository-day without any attributable event has 0 people, a coverage of 0 and NULL in its five concentration values, and every value is rounded only once. More quality gates than planned (no count is NULL, a row with attributable events has at least one person, the top actor is at least the average person, the top 3 hold all events when there are three people or fewer, the concentration values exist exactly when the row has attributable events, the coverage lies between 0 and 100 and exists exactly when the row has events), the names of the gates, the content of the audit row, and the expected events per day. |
| 2026-11-22 | First runs of G3 in Databricks. Status and section 11: the test notebook passed (13 of 13 sections, 174 checks), and the runs for one date and for all dates met every expected value. Section 11 has the results per day and by number of people, and the measured number of person-repository-days. The rows per day of G3 were already in section 18, so the sentence that they were not measured before is corrected, also in cell 11.2 of the notebook. No rule changed. |
| 2026-11-22 | Step 10, G5. Section 6: how a run of G5 works, the scope is always `ALL_DATES`, the source date is only written into the audit row, `NO_INPUT` only without any release event, and an empty table with a warning when no release is an anchor. Section 13: rule 7 is checked only for the earliest event of a release, an hour missing from the manifest is not complete, how the touched hours are counted, rows without an event time go to `missing_event_timestamp`. More quality gates than planned (the key is never NULL, the anchor values are never NULL, the key is the release ID as text, no count is NULL, the percent change has its exact value, the baseline flag fits, the windows fit r, no known commits without an eligible push, no event ID twice), the names of the gates, the content of the audit row, and what is expected on the real week. Section 16: what the G5 test proves instead of the two tests about days. |
| 2026-11-23 | First runs of G5 in Databricks. Status and section 13: the test notebook passed (13 of 13 sections, 179 checks), and the runs with and without a source date met every expected value and gave the same table. Section 13 has the results per day of the release and the measured numbers of baseline-zero rows and rows without activity. The sentence about the first and last 24 hours of the week is made exact, also in the notebook. No rule changed. |
| 2026-11-23 | Step 11, the final Gold audit. Section 15: the name of each check in `failed_checks`, one more check (each kind column of G6 against its Silver table), the rows of each daily table against the repository-days of Silver, how G5 is added up from its last successful run, how the tables are joined, which checks always read whole tables, `NO_INPUT`, the content of the audit row, what is expected on the real week, and what `SUCCEEDED` does not prove. Rule 1 and section 7: the audit reads the Gold tables and writes its row with `gold_table` NULL. Section 16: what the test of the audit proves. |
| 2026-11-23 | First runs of the final Gold audit in Databricks. Status and section 15: the test notebook passed (13 of 13 sections, 176 checks), and the runs with and without a source date passed all 33 checks and met every expected value. Section 15 has the results on the real week |
