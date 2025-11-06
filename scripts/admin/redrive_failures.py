#!/usr/bin/env python
"""Redrive failures from failures queue into retry queue"""

from dataclasses import dataclass
from itertools import batched
from uuid import uuid4

import awswrangler
import boto3
import click
from mypy_boto3_sqs import SQSClient

from common.models import GranuleProcessingEvent


@click.group
@click.option(
    "--environment",
    type=click.Choice(["dev", "prod"]),
    help="Deployed environment",
    required=True,
    envvar="STAGE",
    show_envvar=True,
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="Limit number of messages to redrive (optional)",
)
@click.pass_context
def redrive(ctx: click.Context, environment: str, limit: int | None):
    """Redrive failures into retry queue for reprocessing"""
    sqs = boto3.client("sqs")
    ctx.obj = {
        "environment": environment,
        "sqs": sqs,
        "failure_queue": get_queue_arn(
            sqs, f"hls-vi-historical-orchestration-failures-{environment}"
        ),
        "retry_queue": get_queue_arn(
            sqs, f"hls-vi-historical-orchestration-retry-{environment}"
        ),
        "limit": limit,
    }


@redrive.command()
@click.pass_context
def queue(ctx: click.Context):
    """Redrive failures from DLQ into retry queue"""
    sqs = ctx.obj["sqs"]
    limit = ctx.obj["limit"]
    failure_queue = ctx.obj["failure_queue"]
    retry_queue = ctx.obj["retry_queue"]

    if limit is None:
        click.echo(
            f"Redriving messages from {failure_queue.name} to {retry_queue.name} "
            "via async task."
        )
        response = sqs.start_message_move_task(
            SourceArn=failure_queue.arn,
            DestinationArn=retry_queue.arn,
        )
        click.echo(f"Started redrive task with handle={response['TaskHandle']}")

    else:
        click.echo(
            f"Redriving at most {limit} messages from {failure_queue.name} to "
            f"{retry_queue.name} manually."
        )
        redriven_task_count = 0
        while redriven_task_count < limit:
            click.echo(f"... redrove {redriven_task_count} messages.")

            response = sqs.receive_message(
                QueueUrl=failure_queue.url,
                MaxNumberOfMessages=min(10, limit - redriven_task_count),
                WaitTimeSeconds=10,
            )
            messages = response.get("Messages", [])
            if not messages:
                break

            # store message IDs to message to retain original ReceiptHandle
            message_ids_to_message = {
                message["MessageId"]: message for message in messages
            }

            send_response = sqs.send_message_batch(
                QueueUrl=retry_queue.url,
                Entries=[
                    {
                        "Id": message["MessageId"],
                        "MessageBody": message["Body"],
                    }
                    for message in messages
                ],
            )

            successful_messages_to_delete = [
                {
                    "Id": message["Id"],
                    "ReceiptHandle": message_ids_to_message[message["Id"]][
                        "ReceiptHandle"
                    ],
                }
                for message in send_response.get("Successful", [])
            ]
            sqs.delete_message_batch(
                QueueUrl=failure_queue.url, Entries=successful_messages_to_delete
            )

            for failed in send_response.get("Failed", []):
                click.echo(
                    "Failed to redrive message id={failed['Id']}: {failed['Message']}"
                )

            redriven_task_count += len(successful_messages_to_delete)

        click.echo(f"Completed redriving {redriven_task_count} messages")


@redrive.command()
@click.pass_context
@click.option(
    "--max-attempts",
    type=int,
    default=3,
    show_default=True,
    help="Maximum attempts before giving up retrying granule processing event.",
)
def logs(ctx: click.Context, max_attempts: int):
    """Redrive failures from Athena raw logs table into retry queue"""
    # FIXME: pull table from settings
    sqs = ctx.obj["sqs"]
    limit = ctx.obj["limit"]
    retry_queue = ctx.obj["retry_queue"]
    environment = ctx.obj["environment"]

    if limit is not None and not isinstance(limit, int):
        raise ValueError(f"Limit must be an integer or null (got {limit}")
    limit_clause = f"LIMIT {limit}" if limit is not None else ""

    sql = f"""
    SELECT granule_id, attempt
    FROM "granule-processing-events-failures"
    WHERE attempt < {max_attempts}
    {limit_clause}
    """

    df_iter = awswrangler.athena.read_sql_query(
        sql,
        database=f"hls-vi-historical-logs-{environment}",
        chunksize=1_000,
    )

    n_retried = 0
    for df in df_iter:
        # We can send 10 SQS messages in a batch, so iterate over 10 rows/batch
        for row_batch in batched(df.itertuples(), 10):
            events = [
                GranuleProcessingEvent(
                    granule_id=row.granule_id, attempt=int(row.attempt)
                )
                for row in row_batch
            ]

            sqs.send_message_batch(
                QueueUrl=retry_queue.url,
                Entries=[
                    {
                        "Id": str(uuid4()),
                        "MessageBody": event.to_json(),
                    }
                    for event in events
                ],
            )

            n_retried += len(row_batch)
            if n_retried % 100 == 0:
                click.echo(f"Sent {n_retried} messages to retry queue...")

    click.echo(f"Complete! Retried {n_retried} granule processing events")


@dataclass
class SqsQueue:
    """Queue info"""

    name: str
    url: str
    arn: str


def get_queue_arn(sqs: SQSClient, queue_name: str) -> SqsQueue:
    """Lookup queue ARN by name"""
    queue_url = sqs.get_queue_url(
        QueueName=queue_name,
    )["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    return SqsQueue(name=queue_name, url=queue_url, arn=queue_arn)


if __name__ == "__main__":
    redrive()
