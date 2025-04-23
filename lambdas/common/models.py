from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GranuleProcessingEvent:
    """Event message for granule processing jobs"""

    granule_id: str
    attempt: int = 0

    def to_envvar(self) -> dict[str, str]:
        """Convert this event to environment variable"""
        return {
            "granule_id": self.granule_id,
            "attempt": str(self.attempt),
        }


@dataclass
class InventoryProgressTracker:
    """Tracks our progression through a granule inventory file"""

    # key to the inventory file
    inventory: str
    # how many granules (lines of the file) have we submitted?
    submitted_count: int

    def to_json(self) -> str:
        """Convert to JSON for storage in SSM"""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> InventoryProgressTracker:
        """Parse from JSON"""
        return cls(**json.loads(json_str))
