from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypedDict

import boto3

from common.models import GranuleProcessingEvent, JobOutcome

if TYPE_CHECKING:
    from mypy_boto3_batch.client import BatchClient
    from mypy_boto3_batch.literals import JobStatusType
    from mypy_boto3_batch.type_defs import (
        JobDetailTypeDef,
    )


ACTIVE_JOB_STATUSES: set[JobStatusType] = {
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
}


class JobChangeEvent(TypedDict):
    """Type hint for AWS Batch job change events"""

    version: str
    id: str
    detail_type: str
    source: str
    account: str
    time: str
    region: str
    resources: list[str]
    detail: JobDetailTypeDef


@dataclass
class JobDetails:
    """Container for accessing properties about an AWS Batch job details"""

    detail: JobDetailTypeDef

    @property
    def job_id(self) -> str:
        return self.detail["jobId"]

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

    def get_job_info(self) -> JobDetailTypeDef:
        """Return verbose details about this job"""
        return self.detail

    def get_job_outcome(self) -> JobOutcome:
        """Return the outcome of this job"""
        if self.exit_code == 0:
            return JobOutcome.SUCCESS
        elif self.exit_code is None:
            return JobOutcome.FAILURE_RETRYABLE
        else:
            return JobOutcome.FAILURE_NONRETRYABLE

    def get_granule_event(self) -> GranuleProcessingEvent:
        """Return the granule processing event details for this job"""
        env = {
            entry["name"]: entry["value"]
            for entry in self.detail["container"]["environment"]
            if entry["name"] in {"GRANULE_ID", "ATTEMPT"}
        }
        return GranuleProcessingEvent.from_envvar(env)


@dataclass
class AwsBatchClient:
    """A high level client for interfacing with AWS Batch"""

    queue: str
    job_definition: str
    client: BatchClient = field(default_factory=lambda: boto3.client("batch"))

    def active_jobs_below_threshold(self, threshold: int) -> bool:
        """Ensure active (running/submitted/pending/etc) is below some threshould count

        AWS Batch has some service limits (e.g., 1,000,000 jobs per region
        in the SUBMITTED state) that we need to follow, but mostly this is here to
        avoid flooding our processing queue with a ton of work that might fail.
        """
        paginator = self.client.get_paginator("list_jobs")

        job_count = 0
        for status in ACTIVE_JOB_STATUSES:
            for page in paginator.paginate(
                jobQueue=self.queue,
                jobStatus=status,
            ):
                jobs = page.get("jobSummaryList", [])
                job_count += len(jobs)
                if job_count >= threshold:
                    return False

        return job_count < threshold

    def submit_job(self, event: GranuleProcessingEvent, force_fail: bool) -> str:
        """Submit granule processing event to queue, returning job ID"""
        # TODO: once we're ready, remove the command override
        command = ["/bin/bash", "-c", f"exit {int(force_fail)}"]

        job_name = f"{event.granule_id.replace('.', '-')}_{event.attempt}"
        resp = self.client.submit_job(
            jobDefinition=self.job_definition,
            jobName=job_name,
            jobQueue=self.queue,
            containerOverrides={
                "environment": event.to_environment(),
                "command": command,
            },
        )
        return resp["jobId"]
