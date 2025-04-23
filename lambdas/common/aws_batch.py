from __future__ import annotations

import typing

import boto3
from dataclasses import dataclass, field

if typing.TYPE_CHECKING:
    from mypy_boto3_batch.client import BatchClient
    from mypy_boto3_batch.type_defs import JobDetailTypeDef


@dataclass
class AwsBatchJobDetail:
    """Container for accessing properties about an AWS Batch job details"""

    detail: JobDetailTypeDef

    @property
    def attempts(self) -> int:
        """Return the number of attempts from this job"""
        return len(self.detail.get("attempts", []))

    @property
    def exit_code(self) -> int | None:
        """Get the exit code, if it exists

        Issues from infrastructure (i.e., SPOT interruptions) will not
        have an exit code.
        """
        return self.detail.get("container", {}).get("exitCode")


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
