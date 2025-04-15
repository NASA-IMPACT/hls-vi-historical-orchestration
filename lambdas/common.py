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
