from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Literal

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


INVENTORY_REGEX = re.compile(r".*cumulus_rds_granule.*")
INVENTORY_ROW_REGEX = re.compile(r"^(\S+)\s(.*)\s(\S+)\s(t|f)$")
INVENTORY_ROW_DATE_FORMAT = r"%Y-%m-%d\ %H:%M:%S.%f+00"


@dataclass
class InventoryRow:
    """A row from the inventory report text file

    Each row contains:

    * Granule ID
    * Beginning Date Time
        * If the status is not "published" this may be a `\\N` character
        * If published this will be `%Y-%m%-d\\ %H:%M:%S.%f+00`
    * Status => {completed, failed, queued}
    * Published: true | false
    """

    granule_id: str
    start_datetime: dt.datetime | None
    status: Literal["completed", "failed", "queued"]
    published: bool

    @classmethod
    def parse_line(cls, line: str) -> InventoryRow:
        """Parse a row from the inventory report"""
        if match := INVENTORY_ROW_REGEX.match(line):
            granule_id, maybe_datetime, status, published = match.groups()
            if maybe_datetime == r"\N":
                start_datetime = None
            else:
                start_datetime = dt.datetime.strptime(
                    maybe_datetime, INVENTORY_ROW_DATE_FORMAT
                ).replace(tzinfo=dt.UTC)
            return cls(
                granule_id=granule_id,
                start_datetime=start_datetime,
                status=status,
                published=published == "t",
            )

        raise ValueError(f"Could not parse line ({line})")


@dataclass
class InventoryProgress:
    """Records progression through a granule inventory file"""

    # key to the inventory file
    inventory_key: str
    # how many granules (lines of the file) have we submitted?
    submitted_count: int
    # have we submitted everything?
    is_complete: bool

    @classmethod
    def new(cls, inventory_key: str) -> InventoryProgress:
        """Create a new progress tracker for an inventory object"""
        return cls(
            inventory_key=inventory_key,
            submitted_count=0,
            is_complete=False,
        )

    def to_json(self) -> str:
        """Convert to JSON for storage in SSM"""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> InventoryProgress:
        """Parse from JSON"""
        return cls(**json.loads(json_str))


@dataclass
class InventoryTracking:
    """Records progress through all granule inventory files"""

    inventories: list[InventoryProgress]
    etag: str

    def to_ndjson(self) -> str:
        """Write inventories to a newline delimited JSON"""
        return "\n".join([inventory.to_json() for inventory in self.inventories])

    @classmethod
    def from_ndjson(cls, ndjson: str, etag: str) -> InventoryTracking:
        """Load inventories from NDJSON"""
        inventories = [
            InventoryProgress.from_json(line) for line in ndjson.split("\n") if line
        ]
        return cls(
            inventories=inventories,
            etag=etag,
        )


@dataclass
class InventoryTrackerService:
    """Tracks progress through HLS inventory"""

    bucket: str
    inventories_prefix: str
    inventory_tracking_name: str = "progress.ndjson"
    client: S3Client = field(default_factory=lambda: boto3.client("s3"))

    def _list_inventories(self) -> str:
        """List inventory object keys"""
        inventories = []

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=self.inventories_prefix
        ):
            for item in page.get("Contents", []):
                if INVENTORY_REGEX.match(item["Key"]):
                    inventories.append(item["Key"])

        return inventories

    @property
    def _inventory_tracking_key(self) -> str:
        return f"{self.inventories_prefix.rstrip('/')}/{self.inventory_tracking_name}"

    def create_tracking(self) -> InventoryTracking:
        """Create inventory progress tracking info"""
        inventories = self._list_inventories()
        inventories_progress = InventoryTracking(
            inventories=[
                InventoryProgress(
                    inventory_key=inventory,
                    submitted_count=0,
                    complete=False,
                )
                for inventory in inventories
            ],
            etag="",
        )

        resp = self.client.put_object(
            Bucket=self.bucket,
            Key=self._inventory_tracking_key,
            IfNoneMatch="*",
            Body=inventories_progress.to_ndjson().encode(),
        )
        inventories_progress.etag = resp["ETag"].replace('"', "")

        return inventories_progress

    def get_tracking(self) -> InventoryTracking:
        """Retrieve progress through granule inventories

        If no progress information exists, list inventories and initialize progress
        tracking info.
        """
        try:
            resp = self.client.get_object(
                Bucket=self.bucket,
                Key=self._inventory_tracking_key,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return self.create_progress()
            raise
        else:
            return InventoryTracking.from_ndjson(
                resp["Body"].read().decode(), resp["ETag"]
            )

    def update_tracking(
        self, inventories_progress: InventoryTracking
    ) -> InventoryTracking:
        """Update inventory progress"""
        resp = self.client.put_object(
            Bucket=self.bucket,
            Key=self._inventory_tracking_key,
            IfMatch=inventories_progress.etag,
            Body=inventories_progress.to_ndjson().encode(),
        )
        inventories_progress.etag = resp["ETag"].replace('"', "")

        return inventories_progress
