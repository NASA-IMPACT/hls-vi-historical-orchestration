from __future__ import annotations

import dataclasses
import json
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, cast

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


INVENTORY_REGEX = re.compile(r".*cumulus_rds_granule.*.parquet$")


@dataclass
class InventoryProgress:
    """Records progression through a granule inventory file"""

    # S3 path to the inventory file
    inventory: str
    # how many granules (lines of the file) have we submitted?
    submitted_count: int
    # total count of granules in this invenotry
    total_count: int

    def to_json(self) -> str:
        """Convert to JSON for storage in SSM"""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> InventoryProgress:
        """Parse from JSON"""
        return cls(**json.loads(json_str))

    @property
    def identifier(self) -> str:
        """Identifier for this inventory"""
        return self.inventory.split("/")[-1]

    @property
    def is_complete(self) -> bool:
        """Has the inventory been completely processed?"""
        return self.submitted_count == self.total_count


@dataclass
class InventoryTracking:
    """Records progress through all granule inventory files"""

    inventories: dict[str, InventoryProgress]
    etag: str

    @classmethod
    def new(cls, inventories: list[InventoryProgress], etag: str) -> InventoryTracking:
        """Create new tracking data for inventories"""
        return cls(
            inventories={inventory.identifier: inventory for inventory in inventories},
            etag=etag,
        )

    def to_ndjson(self) -> str:
        """Write inventories to a newline delimited JSON"""
        return "\n".join(
            [inventory.to_json() for inventory in self.inventories.values()]
        )

    @classmethod
    def from_ndjson(cls, ndjson: str, etag: str) -> InventoryTracking:
        """Load inventories from NDJSON"""
        inventories = [
            InventoryProgress.from_json(line) for line in ndjson.split("\n") if line
        ]
        return cls.new(
            inventories=inventories,
            etag=etag,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert into a dict"""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> InventoryTracking:
        """Load from a dict"""
        return cls(
            inventories={
                key: InventoryProgress(**value)
                for key, value in data["inventories"].items()
            },
            etag=data["etag"],
        )

    @property
    def is_complete(self) -> bool:
        """Have all of the inventories been completed?"""
        return all(inventory.is_complete for inventory in self.inventories.values())

    def get_next_inventory(self) -> InventoryProgress | None:
        incomplete_inventories = [
            inventory
            for inventory in self.inventories.values()
            if not inventory.is_complete
        ]
        if incomplete_inventories:
            return incomplete_inventories[0]
        return None

    def increment_progress(self, inventory: InventoryProgress, count: int) -> int:
        """Increment the submitted count for an inventory, returning current count"""
        self.inventories[inventory.identifier].submitted_count += count
        return self.inventories[inventory.identifier].submitted_count


class InventoryTrackingNotFoundError(FileNotFoundError):
    """Raised if the inventory tracking doesn't exist."""


@dataclass
class GranuleTrackerService:
    """Tracks progress through HLS inventory"""

    bucket: str
    inventories_prefix: str
    inventory_tracking_name: str = "progress.ndjson"
    client: S3Client = field(default_factory=lambda: boto3.client("s3"))

    def _list_inventories(self) -> list[str]:
        """List inventory object S3 paths"""
        inventories = []

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=self.inventories_prefix
        ):
            for item in page.get("Contents", []):
                if INVENTORY_REGEX.match(item["Key"]):
                    inventories.append(f"s3://{self.bucket}/{item['Key']}")

        return inventories

    def _create_inventory_progress(self, s3path: str) -> InventoryProgress:
        """Create tracking info for some inventory file on S3"""
        import pyarrow.dataset as ds

        dataset = ds.dataset(s3path)
        scanner = dataset.scanner()
        total_count = scanner.count_rows()
        return InventoryProgress(
            inventory=s3path,
            submitted_count=0,
            total_count=total_count,
        )

    @property
    def _inventory_tracking_key(self) -> str:
        return f"{self.inventories_prefix.rstrip('/')}/{self.inventory_tracking_name}"

    def create_tracking(self) -> InventoryTracking:
        """Create inventory progress tracking info"""
        inventories = self._list_inventories()

        tracking = InventoryTracking.new(
            [self._create_inventory_progress(inventory) for inventory in inventories],
            "",
        )

        resp = self.client.put_object(
            Bucket=self.bucket,
            Key=self._inventory_tracking_key,
            IfNoneMatch="*",
            Body=tracking.to_ndjson().encode(),
        )
        tracking.etag = _sanitize_etag(resp["ETag"])

        return tracking

    def get_tracking(self) -> InventoryTracking:
        """Retrieve progress through granule inventories

        Raises
        ------
        InventoryTrackingNotFoundError
            Raised if the tracking doesn't exist. Please "create()" before
            getting.
        """
        try:
            resp = self.client.get_object(
                Bucket=self.bucket,
                Key=self._inventory_tracking_key,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise InventoryTrackingNotFoundError("No inventory exists yet")
            raise
        else:
            return InventoryTracking.from_ndjson(
                resp["Body"].read().decode(), _sanitize_etag(resp["ETag"])
            )

    def update_tracking(self, tracking: InventoryTracking) -> InventoryTracking:
        """Update inventory progress"""
        resp = self.client.put_object(
            Bucket=self.bucket,
            Key=self._inventory_tracking_key,
            IfMatch=tracking.etag,
            Body=tracking.to_ndjson().encode(),
        )

        updated_tracking = dataclasses.replace(
            tracking, etag=_sanitize_etag(resp["ETag"])
        )

        return updated_tracking

    def get_next_granule_ids(
        self, tracking: InventoryTracking, count: int
    ) -> tuple[InventoryTracking, list[str]]:
        """Return the next <count> granule IDs and updated tracking information"""
        import pyarrow.compute as pc
        import pyarrow.dataset as ds

        if (next_inventory := tracking.get_next_inventory()) is None:
            return tracking, []

        end_row = min(
            next_inventory.submitted_count + count, next_inventory.total_count
        )
        next_rows = list(range(next_inventory.submitted_count, end_row))

        dataset = ds.dataset(next_inventory.inventory)
        table = dataset.scanner(columns=["granule_id", "status"]).take(next_rows)
        completed_granule_ids = table.filter(pc.field("status") == "completed")[
            "granule_id"
        ].to_pylist()

        incremented = tracking.increment_progress(
            next_inventory, end_row - next_inventory.submitted_count
        )
        assert incremented == end_row
        return tracking, cast(list[str], completed_granule_ids)


def _sanitize_etag(etag: str) -> str:
    """Remove extra quota from ETag"""
    return etag.replace('"', "")
