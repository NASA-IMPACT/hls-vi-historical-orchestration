from typing import Literal

from pydantic_settings import BaseSettings


class StackSettings(BaseSettings):
    """Deployment settings for HLS-VI historical processing."""

    STACK_NAME: str
    STAGE: Literal["dev", "prod"]

    MCP_ACCOUNT_ID: str
    MCP_ACCOUNT_REGION: str = "us-west-2"
    MCP_IAM_PERMISSION_BOUNDARY_ARN: str

    VPC_ID: str

    # ----- Buckets
    # Job processing bucket for state (inventories, failures, etc)
    PROCESSING_BUCKET_NAME: str
    PROCESSING_BUCKET_INVENTORY_PREFIX: str = "inventories"
    PROCESSING_BUCKET_LOG_PREFIX: str = "logs"
    PROCESSING_BUCKET_JOB_PREFIX: str = "jobs"

    # LDPAAC private input bucket (*tif files)
    LPDAAC_PROTECTED_BUCKET_NAME: str
    # LPDAAC metadata input bucket (STAC Items & thumbnails)
    LPDAAC_PUBLIC_BUCKET_NAME: str

    # Output bucket for HLS-VI output files
    OUTPUT_BUCKET_NAME: str

    # ----- HLS-VI processing
    PROCESSING_CONTAINER_ECR_URI: str
    # Job vCPU and memory limits
    PROCESSING_JOB_VCPU: int = 1
    PROCESSING_JOB_MEMORY_MB: int = 4_000
    # Custom log group (otherwise they'll land in the catch-all AWS Batch log group)
    PROCESSING_LOG_GROUP_NAME: str
    # Number of internal AWS Batch job retries
    PROCESSING_JOB_RETRY_ATTEMPTS: int = 3

    # TODO: increase instance types allowed
    # Cluster instance types
    BATCH_INSTANCE_TYPES: list[str] = [
        "m6i.xlarge",
        "m6i.2xlarge",
        "m6i.4xlarge",
    ]
    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    # ----- Job feeder
    FEEDER_EXECUTION_SCHEDULE_RATE_MINUTES: int = 60
    FEEDER_MAX_ACTIVE_JOBS: int = 10_000
    FEEDER_GRANULE_SUBMIT_COUNT: int = 50  # 5_000
    FEEDER_JOBS_PER_ARRAY_TASK: int = 1_000

    # ----- Job retry system
    # Send retryable failed AWS Batch jobs to this queue
    JOB_RETRY_QUEUE_NAME: str
    # Failed AWS Batch jobs go to a DLQ that can redrive to the retry queue
    JOB_FAILURE_DLQ_NAME: str
    # Give up requeueing after N attempts
    JOB_RETRY_MAX_ATTEMPTS: int = 3
