from typing import Literal

from pydantic_settings import BaseSettings


class StackSettings(BaseSettings):
    """Deployment settings for HLS-VI historical processing."""

    STACK_NAME: str
    STAGE: Literal["dev", "prod"]

    GCC_ACCOUNT_ID: str
    GCC_ACCOUNT_REGION: str = "us-west-2"

    VPC_ID: str

    # Job processing bucket for state (inventories, failures, etc)
    PROCESSING_BUCKET_NAME: str
    PROCESSING_BUCKET_INVENTORY_PREFIX: str = "inventories"
    PROCESSING_BUCKET_FAILURE_PREFIX: str = "failures"
    PROCESSING_BUCKET_JOB_PREFIX: str = "jobs"

    # LDPAAC granule input bucket
    LPDAAC_GRANULE_BUCKET_NAME: str

    # Output bucket for HLS-VI output files
    OUTPUT_BUCKET_NAME: str

    # AWS Batch processing system
    AWS_BATCH_JOB_QUEUE_NAME: str
    AWS_BATCH_INSTANCE_TYPES: list[str] = [
        "m6i.xlarge",
        "m6i.2xlarge",
    ]

    # Job feeder
    FEEDER_MAX_ACTIVE_JOBS: int = 10_000
    FEEDER_JOBS_PER_ARRAY_TASK: int = 1_000
