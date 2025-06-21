from __future__ import annotations

import datetime as dt
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.dataset as pds
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)


INVENTORY_ROW_REGEX = r"^(?P<granule_id>\S+)\s(?P<start_datetime>.*)\s(?P<status>\S+)\s(?P<published>t|f)$"


INVENTORY_SCHEMA = pa.schema(
    (
        pa.field("granule_id", pa.string()),
        pa.field("start_datetime", pa.date64(), nullable=True),
        pa.field("status", pa.string()),
        pa.field("published", pa.bool_()),
    )
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
    status: str  # should be one of ["completed", "failed", "queued"]
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
    def parse_table(array: pa.StringArray) -> pa.Table:
        """Parse each row of CSV 'contents' from a PyArrow Table"""
        parsed = pc.extract_regex(array, INVENTORY_ROW_REGEX)
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
        start_datetime = pa.array(
            [
                dt.datetime.fromisoformat(s) if s != "NaT" else None
                for s in map(str, start_datetime_str)
            ]
        )

        published = pc.equal(parsed_table_raw["published"], pa.scalar("t"))

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


def consolidate_partitions(
    partitioned_ds: pds.Dataset, output_schema: pa.Schema, destination: Path
) -> None:
    """Read back each partition, sort, and consolidate"""
    logger.info("Consolidating partitions into a final output file")
    path_partition_expressions = {
        Path(fragment.path).parent: fragment.partition_expression
        for fragment in partitioned_ds.get_fragments()
    }

    with pq.ParquetWriter(
        destination, schema=output_schema, compression="snappy"
    ) as writer:
        for path, expr in sorted(path_partition_expressions.items()):
            logger.info(f"Sorting partition {expr} and writing to output")
            table = (
                partitioned_ds.filter(expr)
                .sort_by("start_datetime")
                .to_table(columns=output_schema.names)
            )
            writer.write(table)


def convert_inventory_to_parquet(
    inventories: list[Path], destination: Path, block_size: int = 12_000_000
) -> None:
    """Convert an inventory file to Parquet, sorting by datetime

    Sorting without blowing up memory requires two passes,

    1. Read the input inventory in chunks, sorting each chunk and writing into
       partitions based on datetime
    2. Read each datetime partition, sort it, and append to the final output file.
    """
    partition_schema = pa.schema(
        [
            *INVENTORY_SCHEMA,
            pa.field("year", pa.int16()),
            pa.field("month", pa.int16()),
        ]
    )

    with TemporaryDirectory() as tmp_dir:
        for i_inventory, inventory in enumerate(inventories):
            reader = pcsv.open_csv(
                str(inventory),
                read_options=pcsv.ReadOptions(
                    column_names=["contents"], block_size=block_size
                ),
            )

            for i_chunk, chunk in enumerate(reader):
                logger.info(
                    f"Sorting {inventory} chunk={i_chunk} into partitioned dataset"
                )
                parsed = (
                    InventoryRow.parse_table(cast(pa.StringArray, chunk["contents"]))
                    .filter(pc.field("status") == pa.scalar("completed"))
                    .sort_by("start_datetime")
                )
                parsed = parsed.append_column(
                    "year",
                    pc.year(parsed["start_datetime"]).cast(pa.int16()),
                ).append_column(
                    "month",
                    pc.month(parsed["start_datetime"]).cast(pa.int16()),
                )
                pds.write_dataset(
                    parsed,
                    base_dir=tmp_dir,
                    format="parquet",
                    partitioning_flavor="hive",
                    partitioning=["year", "month"],
                    schema=partition_schema,
                    basename_template=f"tmp-inventory{i_inventory}-chunk{i_chunk}-{{i}}.parquet",
                    existing_data_behavior="overwrite_or_ignore",
                )

        # Open partitioned dataset, sort each partition, and consolidate
        partitioned_ds = pds.dataset(
            tmp_dir, partitioning="hive", schema=partition_schema
        )
        consolidate_partitions(partitioned_ds, INVENTORY_SCHEMA, destination)
        logger.info("Completed parsing, sorting, and writing inventories to Parquet")


def handler(event: dict[str, str], context: Any) -> dict[str, str]:
    """Lambda handler for converting inventory flat files to Parquet

    The event payload is expected to look like,
    ```
    {
        "inventories": [
            "s3://bucket/key/to/inventory/file1",
            "s3://bucket/key/to/inventory/file2"
        ]
    }
    ```
    """
    s3 = boto3.client("s3")

    # Destination
    dest_bucket = os.environ["PROCESSING_BUCKET_NAME"]
    dest_prefix = os.environ["PROCESSING_BUCKET_INVENTORY_PREFIX"].rstrip("/")

    # Parse source inventories into buckets and keys
    src_buckets, src_keys = zip(
        *[s3path.replace("s3://", "").split("/", 1) for s3path in event["inventories"]]
    )
    # remove .sorted* suffix - should just get 1
    src_basename = list({src_key.rsplit(".", 1)[0] for src_key in src_keys})[0]

    dest_key = f"{dest_prefix}/{src_basename}.parquet"

    with TemporaryDirectory() as tmpdir:
        inventory_parquet = Path(tmpdir) / "inventory.parquet"

        inventory_flatfiles = []
        for src_bucket, src_key in zip(src_buckets, src_keys):
            logger.info(f"Processing s3://{src_bucket}/{src_key}")
            inventory_flatfile = Path(tmpdir) / src_key.rsplit("/", 1)[1]
            s3.download_file(src_bucket, src_key, str(inventory_flatfile))
            inventory_flatfiles.append(inventory_flatfile)

        convert_inventory_to_parquet(
            inventory_flatfiles,
            inventory_parquet,
            block_size=int(event.get("block_size", 12_000_000)),
        )

        s3.upload_file(
            str(inventory_parquet),
            dest_bucket,
            dest_key,
        )

    logger.info(
        f"Uploaded Parquet version of {src_key} to s3://{dest_bucket}/{dest_key}"
    )

    return {"bucket": dest_bucket, "key": dest_key}
