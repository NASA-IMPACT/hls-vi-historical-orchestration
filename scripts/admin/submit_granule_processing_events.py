#!/usr/bin/env python
"""Submit granule processing events"""

import json

import boto3
import click


@click.command()
@click.option("--n", type=int, required=True, help="Number of granules to process")
@click.option(
    "--environment",
    type=click.Choice(["dev", "prod"]),
    help="Deployed environment",
    required=True,
    envvar="STAGE",
    show_envvar=True,
)
@click.pass_context
def submit_granule_processing_events(ctx: click.Context, n: int, environment: str):
    """Submit granule processing events to AWS Batch queue

    Note: this doesn't submit processing events directly, but instead uses the
    "queue feeder" Lambda function.
    """
    lambda_ = boto3.client("lambda")

    queue_feeder_arn = get_stack_outputs(
        f"hls-vi-historical-orchestration-{environment}"
    )["QueueFeederLambda"]

    response = lambda_.invoke(
        FunctionName=queue_feeder_arn,
        Payload=json.dumps(
            {
                "granule_submit_count": n,
            }
        ).encode("utf-8"),
    )
    if response["StatusCode"] != 200:
        click.echo("Invocation failed! {response['FunctionError']}")
        click.echo("Logs:")
        for line in response["LogResult"].decode().splitlines():
            click.echo(line)

    click.echo(f"Submitted {n} granules. Output:")
    click.echo(response["Payload"].read())


def get_stack_outputs(stack_name: str) -> dict[str, str]:
    """Return Cloudformation stack outputs"""
    cloudformation = boto3.client("cloudformation")
    response = cloudformation.describe_stacks(StackName=stack_name)
    stack = response["Stacks"][0]
    return {output["OutputKey"]: output["OutputValue"] for output in stack["Outputs"]}


if __name__ == "__main__":
    submit_granule_processing_events()
