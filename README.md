# gharchive-lakehouse-project
End-to-end data engineering project processing GitHub Archive data with AWS S3, Databricks, Spark, Delta Lake, and Medallion Architecture.

## Gold layer

The Gold layer turns the nine Silver event tables into six tables about GitHub repositories. It covers one week of GH Archive data, 1 to 7 February 2024 (UTC): 33,623,791 events in 2,713,169 repositories.

Each table is built in SQL in a Databricks notebook. Quality gates check it before it is published, and a final audit checks it against Silver and against the other tables. The full rules, columns and checks are in [docs/gold_contracts.md](docs/gold_contracts.md).

### The six tables

All six are in the Unity Catalog schema `github_lakehouse.gold`.

| | Table | One row is | Rows in the week |
|---|---|---|---|
| G1 | `repository_delivery_metrics` | One repository on one UTC day with a pull request event | 700,486 |
| G2 | `repository_issue_flow` | One repository on one UTC day with an issue event | 194,627 |
| G3 | `repository_contributor_concentration` | One repository on one UTC day with an engineering event | 4,318,172 |
| G4 | `repository_collaboration_metrics` | One repository on one UTC day with a comment or review event | 444,720 |
| G5 | `release_activity_shift` | One release with a complete 24-hour window before and after it | 108,295 |
| G6 | `repository_activity_profile` | One repository on one UTC day with any event | 5,000,573 |

The key of G1, G2, G3, G4 and G6 is `repo_id` plus `activity_date`. The key of G5 is `repo_id` plus `release_anchor_key`.

#### G1 `repository_delivery_metrics`

**Question:** how many pull request events did a repository have on a day, what share of the closed ones were merged, how long did pull requests take to merge and to close, and how big were they?

- **Reads:** `silver.pull_request_events`.
- **What it does not mean:** the merge share is the share of closed pull request events on that day. It is not the repository's lifetime acceptance rate.
- **Read with care:** look at the median before the average. Over the week the median time to merge is about 13 minutes and the average is 131 hours, because a few very old pull requests pull the average up.

#### G2 `repository_issue_flow`

**Question:** how many issues were opened, closed and reopened on a day, and how long did the closed ones take from creation to close?

- **Reads:** `silver.issue_events`.
- **What it does not mean:** it shows the flow of this week. It does not show how many issues are open in total.
- **Read with care:** the median time to close is about 4 days and the average is 119 days. An issue closed in this week can have been created long before it.

#### G3 `repository_contributor_concentration`

**Question:** how many people did the engineering work in a repository on a day, and how much of it came from the most active ones?

- **Reads:** push, pull request, issue, issue comment, pull request review and pull request review comment events.
- **Measures:** the number of people, the share of the most active person, the share of the three most active people, and the Herfindahl index (HHI) from 0 to 1.
- **What it does not mean:** it is not a measure of work. One event is not one unit of work: a push with 100 commits is one event.
- **Read with care:** 88% of the rows have one person, so their top share is 100 by definition. Bots are kept. Logins that end in `[bot]` produce 17.5% of the events, and the largest accounts are automation with ordinary logins.

#### G4 `repository_collaboration_metrics`

**Question:** how much reviewing and discussing happened in a repository on a day, on what, and by how many people?

- **Reads:** issue comment, pull request review and pull request review comment events.
- **What it does not mean:** an approving review event is not the same as "the pull request was approved", and more comments do not mean better collaboration.
- **Read with care:** 38% of the issue comment events come from logins that end in `[bot]`.

#### G5 `release_activity_shift`

**Question:** how many activity events did a repository have in the 24 hours after a release, compared with the 24 hours before?

- **Reads:** `silver.release_events` for the releases. Push, pull request, issue, watch and fork events for the activity. The source-hour manifest, to prove that every hour of both windows was loaded.
- **Which releases:** the first published event of a release that is not a draft and has a known release ID and published time, whose two windows lie inside the week with every hour complete. 108,295 of the 159,416 release events qualify. The audit row counts the others by reason.
- **What it does not mean:** it is not proof that the release caused the change. The day of the week and the time of day change activity too, and the windows of releases that are close together overlap.
- **Read with care:** 24% of the releases have no activity in the 24 hours before, so their percent change is empty. That is a real result: many releases are published by automation.

#### G6 `repository_activity_profile`

**Question:** how much activity did a repository have on a day, and how is it split over the nine event types?

- **Reads:** all nine Silver tables.
- **What it does not mean:** it is a mix of event counts. It does not measure the amount of work or the health of a project.
- **Read with care:** pushes are 78.9% of all events, so most rows are mostly pushes. Watch events are new stars only.

### Rules every table follows

- **IDs are keys, names are labels.** Tables group and join on `repo_id`. `repo_name` is the latest name seen that day, for display only.
- **NULL is not zero.** NULL means "could not be measured". A percentage whose base is 0 is NULL, not 0.
- **Every average and percentage shows its base.** A column next to it says how many rows it was calculated from.
- **Events are not things.** Four events about one pull request are four events and one pull request. Column names say which one is counted.
- **Days are UTC event days.** A row without an event time is left out and counted in the audit row.
- **Results describe this week.** They are not lifetime rates.

### Limits

- **One week.** Every result is "observed from 1 to 7 February 2024".
- **Small rows.** 59% to 77% of the rows of the daily tables are built from one or two events. A share on such a row is correct but says little. Filter by the event count before you compare shares.
- **Automation.** The repositories and accounts with the most events are automated. Bots are not removed.
- **One thin hour.** On 5 February 2024 from about 19:24 to 20:00 UTC, the source file holds far fewer events than usual. It is the only thin hour of the 168, and it makes the counts of 5 February about 2% low.
- **No check against the raw files in S3.** The S3 connection was removed on purpose after the week was loaded. That every hour is complete rests on the 168 hourly files in Bronze, each loaded once.

### How it is built and checked

Each Gold notebook runs the same six steps:

1. Find the affected days. With a `source_date`, these are the event days of the Silver rows loaded with that date. With a blank date, they are all days.
2. Build the Gold rows of those days from all Silver rows of those days.
3. Write them to a staging copy in `github_lakehouse.gold_staging`.
4. Run the quality gates on the staging copy. If one fails, nothing is published and the notebook stops with an error.
5. Publish: replace exactly those days in the Gold table, in one Delta commit. G5 is always rebuilt whole.
6. Write one row to `github_lakehouse.audit.gold_pipeline_runs`.

So running the same date twice gives the same table, and a late or removed Silver event changes the Gold row it belongs to.

After the six tables, `09_gold_final_audit` runs 33 checks. Every key is unique, the rows and events of each daily table equal Silver, each kind column of G6 equals its Silver table, G5 plus the release events it left out equals all release events, and G6 agrees with G1 to G4 on every repository-day.

Each notebook has a test notebook in `Notebooks/Gold_layer/tests/`. A test writes hand-made Silver rows into a `test_` schema, runs the real notebook on them and compares every value with a result worked out by hand. The tests never touch the production tables.

### How to run it

Gold runs in the Databricks Job `gharchive_lakehouse`, after the Silver tasks. The Job has 19 notebook tasks on serverless compute, and three parameters that reach every notebook as widgets:

| Parameter | Value |
|---|---|
| `source_date` | `YYYY-MM-DD` rebuilds the days of that source date. Blank rebuilds everything |
| `test_schema` | Blank for the production tables |
| `run_id` | `{{job.run_id}}` |

The nine Gold tasks and what each one waits for:

| Task | Notebook | Waits for |
|---|---|---|
| `setup_gold_audit_table` | `08_gold_audit_table` | Nothing |
| `source_hour_manifest` | `07_source_hour_manifest` | `setup_gold_audit_table` |
| `gold_repository_delivery_metrics` | `01_repository_delivery_metrics` | Gold setup; Silver pull requests |
| `gold_repository_issue_flow` | `02_repository_issue_flow` | Gold setup; Silver issues |
| `gold_repository_contributor_concentration` | `03_repository_contributor_concentration` | Gold setup; Silver push, pull request, issue, issue comment, review and review comment |
| `gold_repository_collaboration_metrics` | `04_repository_collaboration_metrics` | Gold setup; Silver issue comment, review and review comment |
| `gold_release_activity_shift` | `05_release_activity_shift` | Gold setup; `source_hour_manifest`; Silver release, push, pull request, issue, watch and fork |
| `gold_repository_activity_profile` | `06_repository_activity_profile` | Gold setup; all nine Silver tasks |
| `gold_final_audit` | `09_gold_final_audit` | The six Gold table tasks |

Keep the Job at one run at a time, because two runs would share the staging copies.

To run the notebooks by hand instead, load Silver first, then run `08`, `07`, `01` to `06`, and `09` last.

To check a run, read its audit rows. Every row should say `SUCCEEDED`.

```sql
SELECT pipeline_name, status, processing_scope, affected_dates,
       gold_rows_published, checks_passed, failed_checks
FROM github_lakehouse.audit.gold_pipeline_runs
WHERE run_id = '<the Job run ID>'
ORDER BY pipeline_name;
```

### Results on the real week

The Job ran for all dates on 24 November 2026 (UTC). Every Gold task succeeded, and the final audit passed 33 of 33 checks.

| Table | Rows | Events |
|---|---|---|
| G1 | 700,486 | 2,173,958 |
| G2 | 194,627 | 548,956 |
| G3 | 4,318,172 | 31,757,399 |
| G4 | 444,720 | 2,497,518 |
| G6 | 5,000,573 | 33,623,791 |

G5 holds 108,295 releases. Together with the 51,121 release events it left out, that is all 159,416 release events in Silver.

Every Gold notebook also passed its test notebook in Databricks, with between 135 and 208 checks each.
