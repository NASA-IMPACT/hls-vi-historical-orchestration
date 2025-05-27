"""HLS-VI historical processing job submission."""

import logging
import os
from typing import Any

from common import (
    AwsBatchClient,
    GranuleProcessingEvent,
    GranuleTrackerService,
    InventoryTrackingNotFoundError,
)

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


def queue_feeder(
    processing_bucket: str,
    inventory_prefix: str,
    output_bucket: str,
    job_queue: str,
    job_definition_name: str,
    max_active_jobs: int,
    granule_submit_count: int,
) -> dict[str, Any]:
    """Submit granule processing jobs to AWS Batch queue"""
    batch = AwsBatchClient(queue=job_queue, job_definition=job_definition_name)
    tracker = GranuleTrackerService(
        bucket=processing_bucket,
        inventories_prefix=inventory_prefix,
    )

    if not batch.active_jobs_below_threshold(max_active_jobs):
        logging.info("Too many active jobs in AWS Batch cluster, exiting early")
        return {}

    try:
        tracking = tracker.get_tracking()
    except InventoryTrackingNotFoundError:
        tracking = tracker.create_tracking()

    updated_tracking, granule_ids = tracker.get_next_granule_ids(
        tracking, granule_submit_count
    )

    for i, granule_id in enumerate(granule_ids):
        processing_event = GranuleProcessingEvent(granule_id=granule_id, attempt=0)
        batch.submit_job(
            event=processing_event,
            output_bucket=output_bucket,
        )

    tracker.update_tracking(updated_tracking)
    return updated_tracking.to_dict()


def handler(event: dict[str, int], context: Any) -> dict[str, Any]:
    """Queue feeder Lambda handler

    The "event" payload contains,
    ```
    {
        "granule_submit_count": 5000,
    }
    ```
    """
    return queue_feeder(
        processing_bucket=os.environ["PROCESSING_BUCKET_NAME"],
        inventory_prefix=os.environ["PROCESSING_BUCKET_INVENTORY_PREFIX"],
        output_bucket=os.environ["OUTPUT_BUCKET"],
        job_queue=os.environ["BATCH_QUEUE_NAME"],
        job_definition_name=os.environ["BATCH_JOB_DEFINITION_NAME"],
        max_active_jobs=int(os.environ["FEEDER_MAX_ACTIVE_JOBS"]),
        granule_submit_count=event["granule_submit_count"],
    )
