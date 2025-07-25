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
    debug: bool = False,
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

    i = 0
    for i, granule_id in enumerate(granule_ids, 1):
        processing_event = GranuleProcessingEvent(granule_id=granule_id, attempt=0)
        batch.submit_job(
            event=processing_event,
            output_bucket=output_bucket,
        )
        if i % 100 == 0:
            logger.info(f"Submitted {i} granule processing events")

    # Don't increment status when running in debug mode
    if not debug:
        tracker.update_tracking(updated_tracking)

    logger.info(f"Completed submitting {i} granule processing events")
    return updated_tracking.to_dict()


def handler(event: dict[str, int], context: Any) -> dict[str, Any]:
    """Queue feeder Lambda handler

    The "event" payload contains,
    ```json
    {
        "granule_submit_count": 5000
    }
    ```

    If this function has a "DEBUG_BUCKET" environment variable defined it will
    upload the results to this bucket instead of the LPDAAC data ingestion bucket.
    This is useful for avoiding triggering ingestion downstream.
    Additionally, granules processed in debug mode will be logged but not counted
    in the granule processing tracking system to ensure we evenutally process the
    granules.
    """
    output_bucket = os.environ["OUTPUT_BUCKET"]
    debug_bucket = os.environ.get("DEBUG_BUCKET")

    return queue_feeder(
        processing_bucket=os.environ["PROCESSING_BUCKET_NAME"],
        inventory_prefix=os.environ["PROCESSING_BUCKET_INVENTORY_PREFIX"],
        output_bucket=debug_bucket or output_bucket,
        job_queue=os.environ["BATCH_QUEUE_NAME"],
        job_definition_name=os.environ["BATCH_JOB_DEFINITION_NAME"],
        max_active_jobs=int(os.environ["FEEDER_MAX_ACTIVE_JOBS"]),
        granule_submit_count=event["granule_submit_count"],
        debug=debug_bucket is not None,
    )
