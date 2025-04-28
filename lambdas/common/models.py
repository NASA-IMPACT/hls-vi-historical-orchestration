from __future__ import annotations

import datetime as dt
import json
from enum import Enum, auto, unique
from typing import Literal

from dataclasses import asdict, dataclass


@unique
class ProcessingOutcome(Enum):
    """Potential outcomes for granule processing"""

    SUCCESS = auto()
    FAILURE = auto()


@unique
class JobOutcome(Enum):
    """Potential outcomes for an AWS Batch job"""

    SUCCESS = auto()
    FAILURE_RETRYABLE = auto()
    FAILURE_NONRETRYABLE = auto()

    @property
    def processing_outcome(self) -> ProcessingOutcome:
        """Current status of the overall processing outcome"""
        return {
            self.SUCCESS: ProcessingOutcome.SUCCESS,
            self.FAILURE_RETRYABLE: ProcessingOutcome.FAILURE,
            self.FAILURE_NONRETRYABLE: ProcessingOutcome.FAILURE,
        }[self]  # type: ignore[index]


HLS_GRANULE_ID_STRFTIME = "%Y%jT%H%M%S"


@dataclass
class GranuleId:
    """Granule identifier"""

    product: Literal["HLS"]
    platform: Literal["L30", "S30"]
    tile: str
    begin_datetime: dt.datetime
    version: Literal["v2.0"]

    @classmethod
    def from_str(cls, granule_id: str) -> GranuleId:
        """Parse components from a string ID"""
        product, platform, tile, begin_datetime, version_major, version_minor = (
            granule_id.split(".")
        )
        return cls(
            product=product,  # type: ignore[arg-type]
            platform=platform,  # type: ignore[arg-type]
            tile=tile,
            begin_datetime=dt.datetime.strptime(
                begin_datetime, HLS_GRANULE_ID_STRFTIME
            ),
            version=".".join([version_major, version_minor]),  # type: ignore[arg-type]
        )

    def to_str(self) -> str:
        """Recombine parts into an ID string"""
        return ".".join(
            [
                self.product,
                self.platform,
                self.tile,
                self.begin_datetime.strftime(HLS_GRANULE_ID_STRFTIME),
                self.version,
            ]
        )


@dataclass(frozen=True)
class GranuleProcessingEvent:
    """Event message for granule processing jobs"""

    granule_id: str
    attempt: int = 0

    def new_attempt(self) -> GranuleProcessingEvent:
        """Return a new GranuleProcessingEvent for another attempt"""
        return GranuleProcessingEvent(
            granule_id=self.granule_id,
            attempt=self.attempt + 1,
        )

    def to_envvar(self) -> dict[str, str]:
        """Convert this event to environment variable"""
        return {
            "GRANULE_ID": self.granule_id,
            "ATTEMPT": str(self.attempt),
        }

    @classmethod
    def from_envvar(cls, env: dict[str, str]) -> GranuleProcessingEvent:
        """Parse from provided environment variables

        Raises
        ------
        KeyError
            Raised if the expected keys aren't in the envvars provided
        """
        return cls(
            granule_id=env["GRANULE_ID"],
            attempt=int(env["ATTEMPT"]),
        )

    def to_environment(self) -> list[dict[str, str]]:
        """Format as a container environment definition"""
        return [
            {"name": key, "value": value} for key, value in self.to_envvar().items()
        ]

    @classmethod
    def from_json(cls, json_str: str) -> GranuleProcessingEvent:
        """Load from a JSON string"""
        data = json.loads(json_str)
        return cls(
            granule_id=data["granule_id"],
            attempt=data["attempt"],
        )

    def to_json(self) -> str:
        """Dump to a JSON string"""
        return json.dumps(asdict(self))
