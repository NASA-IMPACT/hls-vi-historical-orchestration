from __future__ import annotations

import typing

import boto3
from dataclasses import dataclass, field

if typing.TYPE_CHECKING:
    from mypy_boto3_batch.client import BatchClient


@dataclass
class AwsBatchClient:
    """A high level client for interfacing with AWS Batch"""

    client: BatchClient = field(default_factory=lambda: boto3.client("batch"))

    def submitted_jobs_below_threshold(self, queue: str, threshold: int) -> int:
        """Get the number of jobs in the SUBMITTED state

        AWS Batch has a default service limit of 1,000,000 jobs per region
        in the SUBMITTED state. To avoid reaching this service limit without
        having to check every queue, we try to cap the maximum SUBMITTED jobs
        in one queue at a certain threshold.
        """
        paginator = self.client.get_paginator("list_jobs")

        job_count = 0
        for job in paginator.paginate(
            jobQueue=queue,
            jobStatus="SUBMITTED",
        ):
            job_count += 1

        return job_count < threshold

    def submit_job(self, queue: str, job_definition: str, command: list[str]) -> str:
        """Submit command to queue, returning job ID"""
        raise NotImplementedError()
