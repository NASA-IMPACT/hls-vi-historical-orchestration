import json
import os
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from common.models import GranuleId

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


@pytest.fixture
def s3(aws_credentials) -> Iterator[S3Client]:
    """Return a mocked S3 client"""
    with mock_aws():
        yield boto3.client("s3", region_name="us-west-2")


@pytest.fixture
def bucket(s3: S3Client, monkeypatch) -> str:
    """Create our processing bucket, returning bucket name and setting envvar"""
    s3.create_bucket(
        Bucket="foo", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    monkeypatch.setenv("PROCESSING_BUCKET_NAME", "foo")
    return "foo"


@pytest.fixture
def sqs(aws_credentials) -> Iterator[SQSClient]:
    """Return a mocked SQS client"""
    with mock_aws():
        yield boto3.client("sqs", region_name="us-west-2")


@pytest.fixture
def settings(monkeypatch) -> dict[str, str]:
    """Monkeypatch some required settings for test purposes"""
    settings = {
        "JOB_RETRY_QUEUE_NAME": "hls-vi-orch-job-retries",
        "JOB_FAILURE_DLQ_NAME": "hls-vi-orch-job-failure-dlq",
    }
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    return settings


def _queue_url_to_arn(sqs: SQSClient, url: str) -> str:
    """Get the ARN for a queue by name"""
    resp = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return resp["Attributes"]["QueueArn"]


@pytest.fixture
def retry_queue(
    sqs: SQSClient, failure_dlq: str, settings: dict, monkeypatch
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
def failure_dlq(sqs: SQSClient, settings: dict, monkeypatch) -> Iterator[str]:
    """Create mocked failure queue, returning queue URL and populating envvars"""
    queue_name = os.environ["JOB_FAILURE_DLQ_NAME"]
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    monkeypatch.setenv("JOB_FAILURE_DLQ_URL", queue_url)
    yield queue_url
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def granule_id() -> GranuleId:
    """A valid, example granule ID"""
    return GranuleId.from_str("HLS.S30.T01GBH.2022226T214921.v2.0")


@pytest.fixture
def job_detail_failed_error() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of some error"""
    return json.loads((FIXTURES / "job_detail_failed_error.json").read_text())


@pytest.fixture
def job_detail_failed_spot() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of SPOT interruptions"""
    return json.loads((FIXTURES / "job_detail_failed_spot.json").read_text())


@pytest.fixture
def event_job_detail_change_failed() -> dict:
    """AWS Events Job State Change for a AWS Batch job that failed"""
    return json.loads((FIXTURES / "job_state_change_failure.json").read_text())
