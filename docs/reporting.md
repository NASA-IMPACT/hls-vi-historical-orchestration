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
