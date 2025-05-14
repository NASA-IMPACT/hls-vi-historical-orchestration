"""Tests for `queue_feeder` Lambda"""

from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from common import (
    AwsBatchClient,
    GranuleTrackerService,
    InventoryTracking,
)
from queue_feeder.handler import handler


@pytest.fixture
def max_active_jobs(monkeypatch: pytest.MonkeyPatch) -> int:
    """Configure max active jobs"""
    max_active_jobs = 10
    monkeypatch.setenv("FEEDER_MAX_ACTIVE_JOBS", str(max_active_jobs))
    return max_active_jobs


@pytest.fixture
def mocked_list_inventories(local_inventory: Path) -> Iterator[MagicMock]:
    with patch.object(
        GranuleTrackerService,
        "_list_inventories",
        return_value=[str(local_inventory)],
    ) as mock:
        yield mock


@pytest.fixture
def mocked_active_jobs_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[MagicMock]:
    with patch.object(
        AwsBatchClient,
        "active_jobs_below_threshold",
        return_value=True,
    ) as mock:
        yield mock


@pytest.fixture
def mocked_submit_job() -> Iterator[MagicMock]:
    with patch.object(
        AwsBatchClient,
        "submit_job",
        return_value={"jobId": "foo-job-id"},
    ) as mock:
        yield mock


def test_queue_feeder_handler(
    bucket: str,
    output_bucket: str,
    local_inventory: Path,
    mocked_list_inventories: MagicMock,
    mocked_active_jobs_below_threshold: MagicMock,
    mocked_submit_job: MagicMock,
    batch_queue_name: str,
    batch_job_definition: str,
    max_active_jobs: int,
) -> None:
    """Test queue feeder happy path"""
    updated_tracking_data = handler(
        {"granule_submit_count": 2},
        {},
    )
    updated_tracking = InventoryTracking.from_dict(updated_tracking_data)
    assert updated_tracking.inventories[local_inventory.name].submitted_count == 2

    mocked_list_inventories.assert_called_once()
    mocked_active_jobs_below_threshold.assert_called_once()
    assert mocked_submit_job.call_count == 2


def test_queue_feeder_handler_too_many_jobs(
    bucket: str,
    output_bucket: str,
    local_inventory: Path,
    mocked_list_inventories: MagicMock,
    mocked_submit_job: MagicMock,
    batch_queue_name: str,
    batch_job_definition: str,
    max_active_jobs: int,
) -> None:
    """Ensure queue feeder doesn't submit when there's too many jobs"""
    with patch.object(
        AwsBatchClient,
        "active_jobs_below_threshold",
        return_value=False,
    ) as mocked_active_jobs_below_threshold:
        noop = handler(
            {"granule_submit_count": 2},
            {},
        )

    assert noop == {}
    mocked_active_jobs_below_threshold.assert_called()
    mocked_list_inventories.assert_not_called()
    mocked_submit_job.assert_not_called()


def test_queue_feeder_handler_granules_all_done(
    bucket: str,
    output_bucket: str,
    local_inventory: Path,
    granule_tracker_service: GranuleTrackerService,
    mocked_active_jobs_below_threshold: MagicMock,
    mocked_submit_job: MagicMock,
    batch_queue_name: str,
    batch_job_definition: str,
    max_active_jobs: int,
) -> None:
    """Ensure queue feeder doesn't submit when there's no granules to process"""
    with patch.object(
        granule_tracker_service,
        "_list_inventories",
        return_value=[str(local_inventory)],
    ) as mocked_list_inventories:
        # Update tracking info to simulate having completed it already
        tracking = granule_tracker_service.create_tracking()
        while inventory := tracking.get_next_inventory():
            tracking.increment_progress(inventory, inventory.total_count)

        tracking = granule_tracker_service.update_tracking(tracking)
        assert tracking.is_complete

        tracking_data = handler(
            {"granule_submit_count": 5},
            {},
        )

    tracking_complete = InventoryTracking.from_dict(tracking_data)
    assert tracking_complete.is_complete

    mocked_list_inventories.assert_called_once()
    mocked_active_jobs_below_threshold.assert_called_once()
    mocked_submit_job.assert_not_called()
