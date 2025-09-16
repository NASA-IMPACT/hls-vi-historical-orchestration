# HLS-VI Progress Reporting

As a general tip, we usually want to limit our queries to only the latest attempt for a given granule ID. This reduces
duplicate granule counts in our results and ensures we only look at the latest result for a granule ID.

## Query for overall status

We can summarize the status for all granules, organized by the satellite platform (L30, S30) and outcome (success,
failure).

```sql
WITH latest_attempt AS (
    SELECT
        MAX_BY(outcome, attempt) AS outcome,
        ARBITRARY(platform) AS platform
    FROM "hls-vi-historical-logs-prod"."granule_processing_events"
    GROUP BY
        granule_id
)
SELECT platform, outcome, count(*)
FROM latest_attempt
GROUP BY platform, outcome
```

## Granule Progress By Year

The admin scripts in this repository include a tool to create a detailed status report of granule processing outcomes for each platform and acquisition year. The report can be exported as a CSV and is summarized as a bar chart for inclusion in reports.

To run the script, first ensure you're logged into AWS with local credentials. You can then run the report generation
script as:

```shell
# ensure AWS profile is specified
$ AWS_PROFILE=hls-prod
$ uv run python scripts/admin/progress-report.py --environment prod --csv report.csv --plot report.png
Generating report from Athena query...
Creating figure...
Saving...
Complete!
$ head -5 report.csv
platform,outcome,year,granule_count
L30,success,2025-01-01,177442
L30,failure,2025-01-01,17
S30,success,2025-01-01,230386
S30,failure,2025-01-01,24
```
