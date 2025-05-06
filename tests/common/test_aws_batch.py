import pytest
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from pytest_lazy_fixtures import lf

from common import GranuleProcessingEvent, JobDetails, JobOutcome


class TestJobDetail:
    """Tests for JobDetail"""

    @pytest.mark.parametrize(
        ["detail", "attempts"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), 3),
        ],
    )
    def test_attempts(self, detail: JobDetailTypeDef, attempts: int) -> None:
        job_detail = JobDetails(detail)
        assert job_detail.attempts == attempts

    @pytest.mark.parametrize(
        ["detail", "exit_code"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), None),
        ],
    )
    def test_exit_code(self, detail: JobDetailTypeDef, exit_code: int) -> None:
        job_detail = JobDetails(detail)
        assert job_detail.exit_code == exit_code

    def test_get_job_countcome(self, job_detail_failed_error: JobDetailTypeDef) -> None:
        """Test we correctly parse the job outcome"""
        detail = job_detail_failed_error.copy()
        detail["container"]["exitCode"] = 1
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.FAILURE_NONRETRYABLE

        detail = job_detail_failed_error.copy()
        detail["container"]["exitCode"] = 0
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.SUCCESS

        detail = job_detail_failed_error.copy()
        del detail["container"]["exitCode"]
        outcome = JobDetails(detail).get_job_outcome()
        assert outcome == JobOutcome.FAILURE_RETRYABLE

    def test_get_granule_event_details(
        self, job_detail_failed_error: JobDetailTypeDef
    ) -> None:
        """Test we correctly parse the job granule processing event details"""
        detail = job_detail_failed_error.copy()
        detail["container"]["environment"] = [
            {"name": "GRANULE_ID", "value": "foo"},
            {"name": "ATTEMPT", "value": "0"},
        ]
        event_details = JobDetails(detail).get_granule_event()

        assert event_details == GranuleProcessingEvent(granule_id="foo", attempt=0)
