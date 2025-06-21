import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from inventory_converter.handler import InventoryRow, convert_inventory_to_parquet


@pytest.fixture
def inventory() -> dict[str, list[str]]:
    """Example rows from the inventory file categorized by status"""
    return {
        "completed": [
            r"HLS.S30.T01ABC.2024224T215909.v2.0 2024-08-12\ 11:59:50.112+00 completed t",
            r"HLS.S30.T01FBE.2022224T215909.v2.0 2022-08-12\ 21:59:50.112+00 completed t",
        ],
        "queued": [r"HLS.S30.T01GEL.2019059T213751.v2.0 \N queued f"],
        "failed": [
            # sigh, we have a mix of string formats
            r"HLS.S30.T35MNT.2024365T082341.v2.0 2024-12-30\ 08:40:54+00 failed f"
        ],
    }


class TestInventoryRow:
    """Test InventoryRow"""

    def test_parse_line_completed(self, inventory: dict[str, list[str]]) -> None:
        row = InventoryRow.parse_line(inventory["completed"][1])
        assert row.granule_id == "HLS.S30.T01FBE.2022224T215909.v2.0"
        assert row.start_datetime == dt.datetime(
            2022, 8, 12, 21, 59, 50, 112000, tzinfo=dt.UTC
        )
        assert row.status == "completed"
        assert row.published

    def test_parse_line_queued(self, inventory: dict[str, list[str]]) -> None:
        row = InventoryRow.parse_line(inventory["queued"][0])
        assert row.granule_id == "HLS.S30.T01GEL.2019059T213751.v2.0"
        assert row.start_datetime is None
        assert row.status == "queued"
        assert row.published is False

    def test_parse_line_failed(self, inventory: dict[str, list[str]]) -> None:
        row = InventoryRow.parse_line(inventory["failed"][0])
        assert row.granule_id == "HLS.S30.T35MNT.2024365T082341.v2.0"
        assert isinstance(row.start_datetime, dt.datetime)
        assert row.status == "failed"
        assert row.published is False


def test_convert_inventory_to_parquet(
    tmp_path: Path, inventory: dict[str, str]
) -> None:
    """Test conversion of inventory into Parquet"""
    src = tmp_path / "inventory.txt"
    dst = tmp_path / "inventory.parquet"

    with src.open("w") as f:
        for lines in inventory.values():
            for line in lines:
                f.write(f"{line}\n")

    convert_inventory_to_parquet([src], dst)

    df = pd.read_parquet(dst)
    np.testing.assert_array_equal(
        df.columns,
        [
            "granule_id",
            "start_datetime",
            "status",
            "published",
        ],
    )
    np.testing.assert_array_equal(
        df["granule_id"],
        [
            "HLS.S30.T01FBE.2022224T215909.v2.0",
            "HLS.S30.T01ABC.2024224T215909.v2.0",
        ],
    )
    np.testing.assert_array_equal(df["status"], ["completed"] * 2)
    np.testing.assert_array_equal(df["published"], [True] * 2)
    np.testing.assert_array_equal(
        df["start_datetime"],
        df["start_datetime"].sort_values(),
    )
