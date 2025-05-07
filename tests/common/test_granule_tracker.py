from pathlib import Path

import pytest

from common.granule_tracker import (
    GranuleTrackerService,
    InventoryProgress,
    InventoryTracking,
    InventoryTrackingNotFoundError,
)


class TestInventoryProgress:
    """Tests for InventoryProgress"""

    def test_to_from_json(self) -> None:
        progress = InventoryProgress("some-inventory", 0, 10)
        as_json = progress.to_json()
        from_json = InventoryProgress.from_json(as_json)
        assert from_json == progress

    def test_is_complete(self) -> None:
        progress = InventoryProgress("some-inventory", 0, 10)
        assert not progress.is_complete

        progress.submitted_count = progress.total_count
        assert progress.is_complete


class TestInventoryTracking:
    """Tests for InventoryTracking"""

    def test_to_from_njson(self) -> None:
        tracking = InventoryTracking.new(
            inventories=[
                InventoryProgress("sentinel", 0, False),
                InventoryProgress("landsat", 10000, True),
            ],
            etag="asdf",
        )
        as_ndjson = tracking.to_ndjson()
        from_ndjson = InventoryTracking.from_ndjson(as_ndjson, "asdf")
        assert from_ndjson == tracking

    def test_is_complete(self) -> None:
        """Test aggregate 'is_complete'"""
        tracking = InventoryTracking.new(
            inventories=[
                InventoryProgress("sentinel", 0, 10),
                InventoryProgress("landsat", 10, 10),
            ],
            etag="asdf",
        )
        assert not tracking.is_complete

        tracking.inventories["sentinel"].submitted_count = 10
        assert tracking.is_complete


class TestGranuleTrackerService:
    """Tests for GranuleTrackerService"""

    def test_service_list_inventories(
        self, granule_tracker_service: GranuleTrackerService, s3_inventory: str
    ) -> None:
        """Test listing inventory files"""
        inventories = granule_tracker_service._list_inventories()
        assert len(inventories) == 1
        assert inventories[0] == s3_inventory

    def test_create_get_update_tracking(
        self,
        granule_tracker_service: GranuleTrackerService,
        local_inventory: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test sequence of creating/getting/updating inventory"""
        with pytest.raises(InventoryTrackingNotFoundError):
            granule_tracker_service.get_tracking()

        monkeypatch.setattr(
            granule_tracker_service, "_list_inventories", lambda: [str(local_inventory)]
        )

        created_tracking = granule_tracker_service.create_tracking()
        assert len(created_tracking.inventories) == 1
        progress = list(created_tracking.inventories.values())[0]
        assert progress.inventory == str(local_inventory)
        assert progress.submitted_count == 0
        assert created_tracking.etag != ""

        got_tracking = granule_tracker_service.get_tracking()
        assert got_tracking.etag == created_tracking.etag

        got_tracking.increment_progress(progress, 2)
        updated_inventory = granule_tracker_service.update_tracking(got_tracking)
        assert updated_inventory.etag != got_tracking.etag

    def test_get_next_granule_ids(
        self,
        granule_tracker_service: GranuleTrackerService,
        local_inventory: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test fetching next granule IDs and incrementing tracker"""
        monkeypatch.setattr(
            granule_tracker_service,
            "_list_inventories",
            lambda: [str(local_inventory)],
        )

        created_tracking = granule_tracker_service.create_tracking()
        updated_tracking, granule_ids = granule_tracker_service.get_next_granule_ids(
            created_tracking, 2
        )
        assert len(granule_ids) == 2
        progress = list(updated_tracking.inventories.values())[0]
        assert progress.submitted_count == 2
        assert updated_tracking.is_complete is False

        updated_tracking, granule_ids = granule_tracker_service.get_next_granule_ids(
            created_tracking, 2
        )
        assert len(granule_ids) == 1
        assert updated_tracking.is_complete is True

        _, granule_ids = granule_tracker_service.get_next_granule_ids(
            updated_tracking, 1
        )
        assert not granule_ids
