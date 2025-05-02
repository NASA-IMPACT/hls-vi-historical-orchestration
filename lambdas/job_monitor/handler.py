"""HLS-VI historical processing job monitor.

This Lambda monitors and potentially reroutes jobs from AWS Batch for a few scenarios,

* Failures that are retriable (e.g., SPOT interruptions)
    * Routing: Retriable failures go into the requeuer queue for requeueing
    * Logging: Job details are logged to AWS S3
* Failures that are not retriable (e.g., )
    * Routing: These failures need manual intervention and go directly into the DLQ
    * Logging: Job details are logged to AWS S3
* [TODO] Successes are not currently logged


"""

import logging
import os

import boto3

from lambdas.common import (
    JobDetails,
    JobChangeEvent,
    JobOutcome,
)
from lambdas.common.granule_logger import GranuleLoggerService

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def handler(event: JobChangeEvent, context: dict):
    """Event handler for AWS Batch "job state change" events"""
    sqs = boto3.client("sqs")

    logs_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    logs_prefix = os.environ.get("PROCESSING_BUCKET_LOG_PREFIX", "logs")
    retry_queue_url = os.environ["JOB_RETRY_QUEUE_URL"]
    failure_dlq_url = os.environ["JOB_FAILURE_DLQ_URL"]

    details = JobDetails(event["detail"])

    granule_event = details.get_granule_event()
    outcome = details.get_job_outcome()
    logger.info(f"AWS Batch job id={details.job_id} was {outcome}")

    granule_logger = GranuleLoggerService(
        bucket=logs_bucket,
        logs_prefix=logs_prefix,
    )

    granule_logger.put_event_details(details)

    if outcome == JobOutcome.FAILURE_RETRYABLE:
        # FIXME: use a service here
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
        # FIXME: use a service here
        sqs.send_message(
            QueueUrl=failure_dlq_url,
            MessageBody=granule_event.new_attempt().to_json(),
            MessageAttributes={
                "FailureType": {
                    "StringValue": "RETRYABLE",
                    "DataType": "String",
                }
            },
        )
