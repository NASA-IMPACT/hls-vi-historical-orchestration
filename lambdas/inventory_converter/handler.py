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
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

INVENTORY_ROW_REGEX = (
    r"^(?<granule_id>\S+)\s(?<start_datetime>.*)\s(?<status>\S+)\s(?<published>t|f)$"
)


INVENTORY_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        pa.field("granule_id", pa.string()),
        pa.field("start_datetime", pa.date64(), nullable=True),
        pa.field("status", pa.string()),
        pa.field("published", pa.bool_()),
    ]
)


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
                status=status,  # type: ignore[arg-type]
                published=published == "t",
            )

        raise ValueError(f"Could not parse line ({line})")

    @staticmethod
    def parse_table(table: pa.Table) -> pa.Table:
        """Parse each row of CSV 'contents' from a PyArrow Table"""
        parsed = pc.extract_regex(table, INVENTORY_ROW_REGEX)
        parsed_table_raw = pa.Table.from_struct_array(parsed)

        start_datetime_str = pc.replace_substring(
            pc.replace_substring(
                pc.replace_substring(
                    parsed_table_raw["start_datetime"],
                    "\\ ",
                    "T",
                    max_replacements=1,
                ),
                "\\N",
                "NaT",
                max_replacements=1,
            ),
            "+00",
            "Z",
            max_replacements=1,
        )
        # pyarrow is not so good at datetime parsing right now
        start_datetime = pa.array([
            dt.datetime.fromisoformat(s) if s != "NaT" else None
            for s in map(str, start_datetime_str)
        ])

        published = pc.equal(parsed_table_raw["published"], "t")

        parsed_table = pa.Table.from_arrays(
            [
                parsed_table_raw["granule_id"],
                start_datetime,
                parsed_table_raw["status"],
                published,
            ],
            schema=INVENTORY_SCHEMA,
        )

        return parsed_table


def convert_inventory_to_parquet(inventory: str | Path, destination: Path):
    """Convert an inventory file to Parquet"""
    reader = pcsv.open_csv(
        str(inventory), read_options=pcsv.ReadOptions(column_names=["contents"])
    )
    with pq.ParquetWriter(
        destination, INVENTORY_SCHEMA, compression="snappy"
    ) as writer:
        for i, chunk in enumerate(reader):
            logger.info(f"Processing chunk={i}")
            parsed = InventoryRow.parse_table(chunk["contents"])
            writer.write(parsed)
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
    dest_bucket = os.environ["PROCESSING_BUCKET_NAME"]
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
