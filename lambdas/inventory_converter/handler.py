from __future__ import annotations

import datetime as dt
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

INVENTORY_ROW_REGEX = r"^(\S+)\s(.*)\s(\S+)\s(t|f)$"


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
        if match := re.match(INVENTORY_ROW_REGEX, line):
            granule_id, maybe_datetime, status, published = match.groups()
            if maybe_datetime == r"\N":
                start_datetime = None
            else:
                start_datetime = dt.datetime.fromisoformat(
                    maybe_datetime.replace("\\ ", "T").replace("+00", "Z")
                )

            return cls(
                granule_id=granule_id,
                start_datetime=start_datetime,
                status=status,
                published=published == "t",
            )

        raise ValueError(f"Could not parse line ({line})")

    @staticmethod
    def parse_series(series: pd.Series) -> pd.DataFrame:
        """Parse each row from a Pandas Series into Pandas DataFrame"""
        df = series.str.extract(INVENTORY_ROW_REGEX)
        df.columns = ["granule_id", "start_datetime", "status", "published"]

        # sanitize datetime
        df["start_datetime"] = (
            df["start_datetime"]
            .str.replace("\\ ", "T")
            .str.replace("\\N", "NaT")
            .str.replace("+00", "Z")
        )
        df["start_datetime"] = pd.to_datetime(
            df["start_datetime"],
            format="ISO8601",
        )
        df["published"] = df["published"] == "t"
        return df


def convert_inventory_to_parquet(inventory: Path, destination: Path):
    """Convert an inventory file to Parquet"""
    schema = pa.schema(
        [
            pa.field("granule_id", pa.string()),
            pa.field("start_datetime", pa.date64(), nullable=True),
            pa.field("status", pa.string()),
            pa.field("published", pa.bool8()),
        ]
    )

    reader = pd.read_csv(
        inventory,
        iterator=True,
        chunksize=100_000,
        header=None,
        names=["contents"],
    )
    with pq.ParquetWriter(destination, schema, compression="snappy") as writer:
        for i, chunk in enumerate(reader):
            logger.info(f"Processing chunk={i}")
            parsed = InventoryRow.parse_series(chunk["contents"])
            table = pa.Table.from_pandas(parsed, schema=schema, preserve_index=False)
            writer.write(table)
    return destination


def handler(event, context):
    """Lambda handler for converting inventory flat files to Parquet

    The event payload is expected to look like,
    ```
    {
        "bucket": "bucket",
        "key": "/key/to/inventory/file"
    }
    ```
    """
    s3 = boto3.client("s3")

    # Destination
    dest_bucket = os.environ["PROCESSING_BUCKET"]
    dest_prefix = os.environ["PROCESSING_BUCKET_INVENTORY_PREFIX"].rstrip("/")

    # Source
    src_bucket = event["bucket"]
    src_key = event["key"]
    src_basename = src_key.split("/")[-1]

    dest_key = f"{dest_prefix}/{src_basename}.parquet"

    logger.info(f"Processing s3://{src_bucket}/{src_key}")

    with TemporaryDirectory() as tmpdir:
        inventory_flatfile = Path(tmpdir) / "inventory.flat"
        inventory_parquet = Path(tmpdir) / "inventory.parquet"
        s3.download_file(src_bucket, src_key, str(inventory_flatfile))

        convert_inventory_to_parquet(inventory_flatfile, inventory_parquet)

        s3.upload_file(
            inventory_parquet,
            dest_bucket,
            dest_key,
        )

    logger.info(
        f"Uploaded Parquet version of {src_key} to s3://{dest_bucket}/{dest_key}"
    )

    return {"bucket": dest_bucket, "key": dest_key}
