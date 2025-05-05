import pytest
from common.models import (
    GranuleId,
    GranuleProcessingEvent,
    JobOutcome,
    ProcessingOutcome,
)


def test_job_outcome_covers_processing_outcome():
    """Ensure our JobOutcome.processing_outcome covers all ProcessingOutcomes"""
    processing_outcomes = set(ProcessingOutcome)
    job_processing_outcomes = set(outcome.processing_outcome for outcome in JobOutcome)
    assert processing_outcomes == job_processing_outcomes


class TestGranuleId:
    """Tests for GranuleId"""

    @pytest.mark.parametrize(
        "granule_id",
        [
            "HLS.S30.T01GBH.2023051T214901.v2.0",
            "HLS.L30.T18VUJ.2024321T161235.v2.0",
        ],
    )
    def test_to_from_granule_id(self, granule_id: str):
        """Test to/from string"""
        granule_id_ = GranuleId.from_str(granule_id)
        test_granule_id = granule_id_.to_str()
        assert granule_id == test_granule_id


class TestGranuleProcessingEvent:
    """Test GranuleProcessingEvent"""

    def to_from_envvar(self):
        event = GranuleProcessingEvent(
            granule_id="foo",
            attempt=42,
        )
        env = event.to_envvar()
        event_from_envvar = GranuleProcessingEvent.from_envvar(env)

        assert event == event_from_envvar
