import json
import os
from pathlib import Path
from typing import Iterator, cast
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from mypy_boto3_batch import BatchClient
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from common.aws_batch import AwsBatchClient, JobChangeEvent
from common.granule_tracker import GranuleTrackerService
from common.models import GranuleId

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def granule_id() -> GranuleId:
    """A valid, example granule ID"""
    return GranuleId.from_str("HLS.S30.T01GBH.2022226T214921.v2.0")


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Monkeypatch some required settings for test purposes"""
    settings = {
        "JOB_RETRY_QUEUE_NAME": "hls-vi-orch-job-retries",
        "JOB_FAILURE_DLQ_NAME": "hls-vi-orch-job-failure-dlq",
    }
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    return settings


# ==============================================================================
# AWS
@pytest.fixture
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


# ==============================================================================
# S3
@pytest.fixture
def s3(aws_credentials: None) -> Iterator[S3Client]:
    """Return a mocked S3 client"""
    with mock_aws():
        yield boto3.client("s3", region_name="us-west-2")


@pytest.fixture
def bucket(s3: S3Client, monkeypatch: pytest.MonkeyPatch) -> str:
    """Create our processing bucket, returning bucket name and setting envvar"""
    s3.create_bucket(
        Bucket="foo", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", "foo")
    return "foo"


@pytest.fixture
def output_bucket(s3: S3Client, monkeypatch: pytest.MonkeyPatch) -> str:
    """Create our output bucket, returning bucket name and setting envvar"""
    s3.create_bucket(
        Bucket="outputs", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    monkeypatch.setenv("OUTPUT_BUCKET", "output")
    return "output"


# ==============================================================================
# SQS
@pytest.fixture
def sqs(aws_credentials: None) -> Iterator[SQSClient]:
    """Return a mocked SQS client"""
    with mock_aws():
        yield boto3.client("sqs", region_name="us-west-2")


def _queue_url_to_arn(sqs: SQSClient, url: str) -> str:
    """Get the ARN for a queue by name"""
    resp = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return resp["Attributes"]["QueueArn"]


@pytest.fixture
def retry_queue(
    sqs: SQSClient,
    failure_dlq: str,
    settings: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    """Create mocked retry queue, returning queue URL and populating envvars"""
    queue_name = os.environ["JOB_RETRY_QUEUE_NAME"]
    failure_dlq_arn = _queue_url_to_arn(sqs, failure_dlq)
    queue_url = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": failure_dlq_arn,
                    "maxReceiveCount": 1,
                }
            )
        },
    )["QueueUrl"]
    monkeypatch.setenv("JOB_RETRY_QUEUE_URL", queue_url)
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def failure_dlq(
    sqs: SQSClient, settings: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    """Create mocked failure queue, returning queue URL and populating envvars"""
    queue_name = os.environ["JOB_FAILURE_DLQ_NAME"]
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    monkeypatch.setenv("JOB_FAILURE_DLQ_URL", queue_url)
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


# ==============================================================================
# AWS Batch
@pytest.fixture
def batch(aws_credentials: None) -> Iterator[BatchClient]:
    """AWS Batch client"""
    with mock_aws():
        yield boto3.client("batch", region_name="us-west-2")


@pytest.fixture
def batch_queue_name(monkeypatch: pytest.MonkeyPatch) -> str:
    """AWS Batch queue name envvar"""
    queue_name = "hls-vi-processing"
    monkeypatch.setenv("BATCH_QUEUE_NAME", queue_name)
    return queue_name


@pytest.fixture
def batch_job_definition(monkeypatch: pytest.MonkeyPatch) -> str:
    """AWS Batch JobDefinition envvar"""
    job_definition_name = "hls-vi-processing"
    monkeypatch.setenv("BATCH_JOB_DEFINITION_NAME", job_definition_name)
    return job_definition_name


@pytest.fixture
def job_detail_failed_error() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of some error"""
    data = json.loads((FIXTURES / "job_detail_failed_error.json").read_text())
    return cast(JobDetailTypeDef, data)


@pytest.fixture
def job_detail_failed_spot() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of SPOT interruptions"""
    data = json.loads((FIXTURES / "job_detail_failed_spot.json").read_text())
    return cast(JobDetailTypeDef, data)


@pytest.fixture
def event_job_detail_change_failed() -> JobChangeEvent:
    """AWS Events Job State Change for a AWS Batch job that failed"""
    data = json.loads((FIXTURES / "job_state_change_failure.json").read_text())
    return cast(JobChangeEvent, data)


# ==============================================================================
# GranuleTrackerService
@pytest.fixture
def granule_tracker_service(
    bucket: str, monkeypatch: pytest.MonkeyPatch
) -> GranuleTrackerService:
    """Create service with mocked S3 client and bucket pre-created"""
    monkeypatch.setenv("PROCESSING_BUCKET_INVENTORY_PREFIX", "inventories")
    return GranuleTrackerService(
        bucket=bucket,
        inventories_prefix="inventories",
    )


@pytest.fixture
def local_inventory(
    granule_tracker_service: GranuleTrackerService, tmp_path: Path, s3: S3Client
) -> Path:
    """Create a fake granule inventory file"""
    inventory_contents = [
        [
            "HLS.S30.T01FBE.2022224T215909.v2.0",
            "2022-08-12T21:59:50.112Z",
            "completed",
            True,
        ],
        [
            "HLS.S30.T01GEL.2019059T213751.v2.0",
            "2019-02-28T21:37:51.123Z",
            "completed",
            True,
        ],
        [
            "HLS.S30.T35MNT.2024365T082341.v2.0",
            "2024-12-30T08:40:54.243Z",
            "completed",
            True,
        ],
    ]

    df = pd.DataFrame(
        inventory_contents,
        columns=["granule_id", "start_datetime", "status", "published"],
    )
    df["start_datetime"] = pd.to_datetime(df["start_datetime"])

    parquet_file = tmp_path / "inventory.parquet"
    df.to_parquet(parquet_file)
    return parquet_file


@pytest.fixture
def s3_inventory(local_inventory: Path, bucket: str, s3: S3Client) -> str:
    """Create a fake granule inventory on S3"""
    key = "inventories/PROD_sentinel_cumulus_rds_granule_blah.sorted.parquet"
    s3.upload_file(
        str(local_inventory),
        Bucket=bucket,
        Key=key,
    )
    return f"s3://{bucket}/{key}"


# ===== AwsBatchClient
@pytest.fixture
def mocked_batch_client_submit_job() -> Iterator[MagicMock]:
    with patch.object(
        AwsBatchClient,
        "submit_job",
        return_value="foo-job-id",
    ) as mock:
        yield mock
