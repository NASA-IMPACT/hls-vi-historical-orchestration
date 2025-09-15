# HLS-VI Historical Orchestration Runbook

## Pre-requisite: Convert LPDAAC inventories to Parquet

The inventory we received from LPDAAC was in a fairly inconsistent text delimited format. In order to process the
granules more easily we need to preprocess these input files to achieve the following goals:

- Fix inconsistent formatting.
- Facilitate fast access by converting to binary columnar format (Parquet).
- Combine 3 granule inventory files into just one for easier tracking.
- Subset to just the granules that have been successfully ingested.

The payload to convert the inventories looks like:

```json
{
    "inventories": [
        "s3://hls-vi-historical-orchestration-dev/granule-inventories/raw/PROD_HLS_20250324_1350_cumulus_rds_granules_HLS.L30.v2.0.sorted",
        "s3://hls-vi-historical-orchestration-dev/granule-inventories/raw/PROD_HLS_20250324_1502_cumulus_rds_granules_HLS.S30.v2.0.sortedaa",
        "s3://hls-vi-historical-orchestration-dev/granule-inventories/raw/PROD_HLS_20250324_1502_cumulus_rds_granules_HLS.S30.v2.0.sortedab"
    ],
    "destination": "s3://hls-vi-historical-orchestration-dev/granule-inventories/combined-cumulus-rds-granules-20250324.parquet"
}
```

## Pausing Granule Processing

The "Queue Feeder" is scheduled using an EventBridge rule that can be disabled to temporarily pause processing. This
could be done using the CLI or a client library like Boto3, but it's perhaps most straightforward to manually disable
the rule within the AWS Console. You can either find the EventBridge Rule using the EventBridge page, or find the
EventBridge rule by finding the Queue Feeder Lambda function and clicking on the trigger.

## Manually Submit Granules from Inventory

We can manually submit a batch of granules to be processed by interacting with the "Queue Feeder" Lambda function. The
payload required looks like:

```json
{
    "granule_submit_count": 10
}
```

You can also use the admin script in this repository to submit the granules:

```shell
# You need to be logged into production for this to work
$ export AWS_PROFILE=hls-prod
# You probably also want to tell UV to load the dotenv for your environment
$ export UV_ENV_FILE=.env.prod
$ uv run scripts/admin/submit_granule_processing_events.py --environment prod --n 10
```

## Requeuing Recently Failed Granules

Sometimes a granule might fail to process for ephemeral issues specific to the instance or the networking to the
instance. These cases do not look "retryable" based on their exit code so we don't automatically retry them. We might
also have a bug in our software that we resolve and deploy. In both of these cases we want to reprocess the granules
that have failed.

If the issue happened within the last 14 days there should be a message in our "Failure Queue" that records the
`GranuleProcessingEvent` for any failed granules. For this case we can easily resubmit these granules to the "Job
Processing System" via the "Requeuer" by sending these messages to the "Retry Queue".

The first pathway for this is using an admin script, `scripts/admin/redrive_failures.py`. This script allows you to
redrive the entire queue or a subset of `N` messages. For example,

```shell
# You need to be logged into production for this to work
$ export AWS_PROFILE=hls-prod
# You probably also want to tell UV to load the dotenv for your environment
$ export UV_ENV_FILE=.env.prod
# To redrive everything...
$ uv run scripts/admin/redrive_failures.py
Redriving messages from hls-vi-historical-orchestration-failures-prod to hls-vi-historical-orchestration-retry-prod via async task.
Started redrive task with handle=<redrive task handle>
# OR to redrive only a few messages
$ uv run scripts/admin/redrive_failures.py --limit 10
Redriving at most 10 messages from hls-vi-historical-orchestration-failures-prod to hls-vi-historical-orchestration-retry-prod manually.
... redrove 0 messages.
Completed redriving 1 messages
```

You could also use the AWS Console by "redriving" the "Failure Queue" to the "Retry Queue". The "Failure Queue" isn't
configured as the Dead-Letter Queue of the "Retry Queue" within AWS, but you can still specify the "Retry Queue"
manually as part of creating a queue "redrive task".
