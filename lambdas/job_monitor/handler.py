"""HLS-VI historical processing job monitor."""

from common import AwsBatchJobDetail


def handler(event, context):
    print(f"Received event {event}")
