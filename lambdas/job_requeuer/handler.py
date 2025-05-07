"""HLS-VI historical processing job requeuer."""

from typing import Any

from common import GranuleProcessingEvent  # noqa: F401


def handler(event: dict[str, str], context: Any) -> None:
    print(f"Received event {event}")
