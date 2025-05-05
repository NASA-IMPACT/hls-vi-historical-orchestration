"""HLS-VI historical processing job monitor."""
from common import GranuleProcessingEvent  # noqa: F401


def handler(event, context):
    print(f"Received event {event}")
