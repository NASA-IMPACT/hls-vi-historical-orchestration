from pathlib import Path

import pandas as pd
import pytest
from mypy_boto3_s3.client import S3Client

from common.granule_tracker import (
    InventoryProgress,
    InventoryTracking,
    InventoryTrackerService,
    InventoryTrackingNotFoundError,
)


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
        tracking = InventoryTracking(
            inventories=[
                InventoryProgress("sentinel", 0, 10),
                InventoryProgress("landsat", 10, 10),
            ],
            etag="asdf",
        )
        assert not tracking.is_complete

        tracking.inventories[0].submitted_count = 10
        assert tracking.is_complete


class TestInventoryTrackerService:
    """Tests for InventoryTrackerService"""

    @pytest.fixture
    def service(self, bucket: str) -> InventoryTrackerService:
        """Create service with mocked S3 client and bucket pre-created"""
        return InventoryTrackerService(
            bucket=bucket,
            inventories_prefix="inventories",
        )

    @pytest.fixture
    def inventory(
        self, service: InventoryTrackerService, tmp_path: Path, s3: S3Client
    ) -> str:
        """Create a fake inventory file"""
        inventory_contents = [
            [
                "HLS.S30.T01FBE.2022224T215909.v2.0",
                "2022-08-12T21:59:50.112Z",
                "completed",
                True,
            ],
            ["HLS.S30.T01GEL.2019059T213751.v2.0", "NaT", "queued", False],
            [
                "HLS.S30.T35MNT.2024365T082341.v2.0",
                "2024-12-30T08:40:54.243Z",
                "failed",
                False,
            ],
        ]

        df = pd.DataFrame(
            inventory_contents,
            columns=["granule_id", "start_datetime", "status", "published"],
        )
        df["start_datetime"] = pd.to_datetime(df["start_datetime"])

        parquet_file = tmp_path / "inventory.parquet"
        df.to_parquet(parquet_file)

        key = "inventories/PROD_sentinel_cumulus_rds_granule_blah.sorted.parquet"
        s3.upload_file(
            str(parquet_file),
            Bucket=service.bucket,
            Key=key,
        )
        return f"s3://{service.bucket}/{key}"

    def test_service_list_inventories(
        self, service: InventoryTrackerService, inventory: str
    ):
        """Test listing inventory files"""
        inventories = service._list_inventories()
        assert len(inventories) == 1
        assert inventories[0] == inventory

    def test_create_get_update_tracking(
        self, service: InventoryTrackerService, inventory: str
    ):
        """Test sequence of creating/getting/updating inventory"""
        with pytest.raises(InventoryTrackingNotFoundError):
            service.get_tracking()

        created_tracking = service.create_tracking()
        assert len(created_tracking.inventories) == 1
        assert created_tracking.inventories[0].inventory_key == inventory
        assert created_tracking.inventories[0].submitted_count == 0
        assert created_tracking.etag != ""

        got_tracking = service.get_tracking()
        assert got_tracking.etag == created_tracking.etag

        got_tracking.inventories[0].submitted_count += 100
        updated_inventory = service.update_tracking(got_tracking)
        assert updated_inventory.etag != got_tracking.etag
