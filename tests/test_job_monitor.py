"""Tests for `job_monitor` Lambda"""

from job_monitor.handler import handler
from mypy_boto3_sqs import SQSClient


def test_handler_logs_nonretryable_failure(
    bucket: str, sqs: SQSClient, failure_dlq: str, event_job_detail_change_failed: dict
):
    """Test the handler"""
    handler(event_job_detail_change_failed, {})

    messages = sqs.receive_message(QueueUrl=failure_dlq)["Messages"]
    assert len(messages) == 1


def test_handler_logs_retryable_failure(
    bucket: str,
    sqs: SQSClient,
    retry_queue: str,
    event_job_detail_change_failed: dict,
    job_detail_failed_spot: dict,
):
    """Test the handler"""
    event = event_job_detail_change_failed.copy()
    event["detail"] = job_detail_failed_spot
    handler(event, {})

    messages = sqs.receive_message(QueueUrl=retry_queue)["Messages"]
    assert len(messages) == 1


def test_handler_logs_success(
    bucket: str, sqs: SQSClient, failure_dlq: str, event_job_detail_change_failed: dict
):
    """Test the handler"""
    handler(event_job_detail_change_failed, {})
