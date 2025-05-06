from pathlib import Path

import pandas as pd
import pytest
from common.granule_tracker import (
    GranuleTrackerService,
    InventoryProgress,
    InventoryTracking,
    InventoryTrackingNotFoundError,
)
from mypy_boto3_s3.client import S3Client


class TestInventoryProgress:
    """Tests for InventoryProgress"""

    def test_to_from_json(self):
        progress = InventoryProgress("some-inventory", 0, 10)
        as_json = progress.to_json()
        from_json = InventoryProgress.from_json(as_json)
        assert from_json == progress

    def test_is_complete(self):
        progress = InventoryProgress("some-inventory", 0, 10)
        assert not progress.is_complete

        progress.submitted_count = progress.total_count
        assert progress.is_complete


class TestInventoryTracking:
    """Tests for InventoryTracking"""

    def to_from_njson(self):
        tracking = InventoryTracking(
            inventories=[
                InventoryProgress("sentinel", 0, False),
                InventoryProgress("landsat", 10000, True),
            ],
            etag="asdf",
        )
        as_ndjson = tracking.to_ndjson()
        from_ndjson = InventoryTracking.from_ndjson(as_ndjson)
        assert from_ndjson == tracking

    def test_is_complete(self):
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

    @pytest.fixture
    def service(self, bucket: str) -> GranuleTrackerService:
        """Create service with mocked S3 client and bucket pre-created"""
        return GranuleTrackerService(
            bucket=bucket,
            inventories_prefix="inventories",
        )

    @pytest.fixture
    def local_inventory(
        self, service: GranuleTrackerService, tmp_path: Path, s3: S3Client
    ) -> str:
        """Create a fake granule inventory file"""
        inventory_contents = [
            [
                "HLS.S30.T01FBE.2022224T215909.v2.0",
                "2022-08-12T21:59:50.112Z",
                "completed",
                True,
            ],
            [
                "HLS.S30.T01GEL.2019059T213751.v2.0",
                "2019-02-28T21:37:51.123Z",
                "completed",
                True,
            ],
            [
                "HLS.S30.T35MNT.2024365T082341.v2.0",
                "2024-12-30T08:40:54.243Z",
                "completed",
                True,
            ],
        ]

        df = pd.DataFrame(
            inventory_contents,
            columns=["granule_id", "start_datetime", "status", "published"],
        )
        df["start_datetime"] = pd.to_datetime(df["start_datetime"])

        parquet_file = tmp_path / "inventory.parquet"
        df.to_parquet(parquet_file)
        return str(parquet_file)

    @pytest.fixture
    def s3_inventory(
        self, local_inventory: str, service: GranuleTrackerService, s3: S3Client
    ) -> str:
        """Create a fake granule inventory on S3"""
        key = "inventories/PROD_sentinel_cumulus_rds_granule_blah.sorted.parquet"
        s3.upload_file(
            local_inventory,
            Bucket=service.bucket,
            Key=key,
        )
        return f"s3://{service.bucket}/{key}"

    def test_service_list_inventories(
        self, service: GranuleTrackerService, s3_inventory: str
    ):
        """Test listing inventory files"""
        inventories = service._list_inventories()
        assert len(inventories) == 1
        assert inventories[0] == s3_inventory

    def test_create_get_update_tracking(
        self,
        service: GranuleTrackerService,
        local_inventory: str,
        monkeypatch,
    ):
        """Test sequence of creating/getting/updating inventory"""
        with pytest.raises(InventoryTrackingNotFoundError):
            service.get_tracking()

        monkeypatch.setattr(service, "_list_inventories", lambda: [local_inventory])

        created_tracking = service.create_tracking()
        assert len(created_tracking.inventories) == 1
        progress = list(created_tracking.inventories.values())[0]
        assert progress.inventory == local_inventory
        assert progress.submitted_count == 0
        assert created_tracking.etag != ""

        got_tracking = service.get_tracking()
        assert got_tracking.etag == created_tracking.etag

        got_tracking.increment_progress(progress, 2)
        updated_inventory = service.update_tracking(got_tracking)
        assert updated_inventory.etag != got_tracking.etag

    def test_get_next_granule_ids(
        self,
        service: GranuleTrackerService,
        local_inventory: str,
        monkeypatch,
    ):
        """Test fetching next granule IDs and incrementing tracker"""
        monkeypatch.setattr(service, "_list_inventories", lambda: [local_inventory])

        created_tracking = service.create_tracking()
        updated_tracking, granule_ids = service.get_next_granule_ids(
            created_tracking, 2
        )
        assert len(granule_ids) == 2
        progress = list(updated_tracking.inventories.values())[0]
        assert progress.submitted_count == 2
        assert updated_tracking.is_complete is False

        updated_tracking, granule_ids = service.get_next_granule_ids(
            created_tracking, 2
        )
        assert len(granule_ids) == 1
        assert updated_tracking.is_complete is True

        _, granule_ids = service.get_next_granule_ids(updated_tracking, 1)
        assert not granule_ids
