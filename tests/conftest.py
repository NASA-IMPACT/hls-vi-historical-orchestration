import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_batch.type_defs import JobDetailTypeDef

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Return a mocked S3 client"""
    with mock_aws():
        yield boto3.client("s3", region_name="us-west-2")


@pytest.fixture(scope="function")
def bucket(s3) -> str:
    """Create a bucket, returning bucket name"""
    s3.create_bucket(
        Bucket="foo", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    return "foo"


@pytest.fixture
def job_detail_failed_error() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of some error"""
    return json.loads((FIXTURES / "job_detail_failed_error.json").read_text())


@pytest.fixture
def job_detail_failed_spot() -> JobDetailTypeDef:
    """DescribeJob for a AWS Batch job that failed because of SPOT interruptions"""
    return json.loads((FIXTURES / "job_detail_failed_spot.json").read_text())
