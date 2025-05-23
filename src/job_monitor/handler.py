"""HLS-VI historical processing job monitor.

This Lambda monitors and potentially reroutes jobs from AWS Batch for a few scenarios,

* Failures that are retriable (e.g., SPOT interruptions)
    * Routing: Retriable failures go into the requeuer queue for requeueing
    * Logging: Job details are logged to S3
* Failures that are not retriable (e.g., a bug in our code)
    * Routing: These failures need manual intervention and go directly into the DLQ
    * Logging: Job details are logged to S3
* Successes
    * Routing: These jobs are complete! No further steps are taken.
    * Logging: Job details are logged to S3

"""

import logging
import os
from typing import Any

import boto3

from common import (
    JobChangeEvent,
    JobDetails,
    JobOutcome,
)
from common.granule_logger import GranuleLoggerService

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def job_monitor(
    logs_bucket: str,
    logs_prefix: str,
    retry_queue_url: str,
    failure_dlq_url: str,
    job_change_event: JobChangeEvent,
) -> None:
    """Handle job failure and success state change events

    All job outcomes (successes, failures, and retryable failures) are logged.

    Retryable failures (e.g., SPOT interruptions) are re-routed to a queue for the
    "requeuer" to pickup.

    Non-retryable failures (non-zero exit codes) are routed to a DLQ for triage
    and redriving into the "requeuer"'s queue.
    """
    sqs = boto3.client("sqs")

    details = JobDetails(job_change_event["detail"])

    granule_event = details.get_granule_event()
    outcome = details.get_job_outcome()
    logger.info(f"AWS Batch job id={details.job_id} was {outcome}")

    granule_logger = GranuleLoggerService(
        bucket=logs_bucket,
        logs_prefix=logs_prefix,
    )

    granule_logger.put_event_details(details)

    if outcome == JobOutcome.FAILURE_RETRYABLE:
        sqs.send_message(
            QueueUrl=retry_queue_url,
            MessageBody=granule_event.new_attempt().to_json(),
            MessageAttributes={
                "FailureType": {
                    "StringValue": "RETRYABLE",
                    "DataType": "String",
                }
            },
        )
    elif outcome == JobOutcome.FAILURE_NONRETRYABLE:
        sqs.send_message(
            QueueUrl=failure_dlq_url,
            MessageBody=granule_event.new_attempt().to_json(),
            MessageAttributes={
                "FailureType": {
                    "StringValue": "NONRETRYABLE",
                    "DataType": "String",
                }
            },
        )


def handler(event: JobChangeEvent, context: Any) -> None:
    """Event handler for AWS Batch "job state change" events"""
    return job_monitor(
        logs_bucket=os.environ["PROCESSING_BUCKET_NAME"],
        logs_prefix=os.environ.get("PROCESSING_BUCKET_LOG_PREFIX", "logs"),
        retry_queue_url=os.environ["JOB_RETRY_QUEUE_URL"],
        failure_dlq_url=os.environ["JOB_FAILURE_DLQ_URL"],
        job_change_event=event,
    )
