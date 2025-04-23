import datetime as dt

import pytest
from mypy_boto3_s3.client import S3Client

from common.granule_inventory import (
    InventoryRow,
    InventoryProgress,
    InventoryTracking,
    InventoryTrackerService,
)


class TestInventoryRow:
    """Test InventoryRow"""

    def test_parse_line_completed(self):
        line = r"HLS.S30.T01FBE.2022224T215909.v2.0 2022-08-12\ 21:59:50.112+00 completed t"
        row = InventoryRow.parse_line(line)
        assert row.granule_id == "HLS.S30.T01FBE.2022224T215909.v2.0"
        assert row.start_datetime == dt.datetime(2022, 8, 12, 21, 59, 50, 112000, tzinfo=dt.UTC)
        assert row.status == "completed"
        assert row.published

    def test_parse_line_queued(self):
        line = r"HLS.S30.T01GEL.2019059T213751.v2.0 \N queued f"
        row = InventoryRow.parse_line(line)
        assert row.granule_id == "HLS.S30.T01GEL.2019059T213751.v2.0"
        assert row.start_datetime is None
        assert row.status == "queued"
        assert row.published is False

    def test_parse_line_failed(self):
        line = r"HLS.S30.T35MNT.2024365T082341.v2.0 2024-12-30\ 08:40:54.243+00 failed f"
        row = InventoryRow.parse_line(line)
        assert row.granule_id == "HLS.S30.T35MNT.2024365T082341.v2.0"
        assert isinstance(row.start_datetime, dt.datetime)
        assert row.status == "failed"
        assert row.published is False


class TestInventoryProgress:
    """Tests for InventoryProgress"""

    def test_new(self):
        progress = InventoryProgress.new("some-inventory")
        assert progress.submitted_count == 0
        assert progress.is_complete is False

    def to_from_json(self):
        progress = InventoryProgress("some-inventory", 0, False)
        as_json = progress.to_json()
        from_json = InventoryProgress.from_json(as_json)
        assert from_json == progress


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
    def inventory(self, service: InventoryTrackerService, s3: S3Client) -> str:
        """Create a fake inventory file"""
        inventory_contents = [
            r"HLS.S30.T01FBE.2022224T215909.v2.0 2022-08-12\ 21:59:50.112+00 completed t",
            r"HLS.S30.T01GEL.2019059T213751.v2.0 \N queued f",
            r"HLS.S30.T35MNT.2024365T082341.v2.0 2024-12-30\ 08:40:54.243+00 failed f",
        ]
        key = "inventories/PROD_sentinel_cumulus_rds_granule_blah.sorted"
        s3.put_object(
            Bucket=service.bucket,
            Key=key,
            Body="\n".join(inventory_contents).encode(),
        )
        return key

    def test_service_list_inventories(self, service: InventoryTrackerService, inventory: str):
        """Test listing inventory files"""
        inventories = service._list_inventories()
        assert len(inventories) == 1
        assert inventories[0] == inventory
