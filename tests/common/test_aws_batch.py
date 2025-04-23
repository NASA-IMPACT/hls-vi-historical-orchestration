import pytest
from mypy_boto3_batch.type_defs import JobDetailTypeDef
from pytest_lazy_fixtures import lf

from common.aws_batch import AwsBatchJobDetail


class TestAwsBatchJobDetail:
    """Tests for AwsBatchJobDetail"""

    @pytest.mark.parametrize(
        ["detail", "attempts"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), 3),
        ],
    )
    def test_attempts(self, detail: JobDetailTypeDef, attempts: int):
        job_detail = AwsBatchJobDetail(detail)
        assert job_detail.attempts == attempts

    @pytest.mark.parametrize(
        ["detail", "exit_code"],
        [
            (lf("job_detail_failed_error"), 1),
            (lf("job_detail_failed_spot"), None),
        ],
    )
    def test_exit_code(self, detail: JobDetailTypeDef, exit_code: int):
        job_detail = AwsBatchJobDetail(detail)
        assert job_detail.exit_code == exit_code
