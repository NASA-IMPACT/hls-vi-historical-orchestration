from typing import Any, Literal

from aws_cdk import (
    Aws,
    Duration,
    Size,
    aws_batch,
    aws_ecs,
    aws_iam,
    aws_logs,
)
from constructs import Construct


def ecr_uri_to_repo_arn(uri: str) -> str | None:
    """Convert an ECR container URI to the ARN for the repository

    This returns "None" if the container URI is not in ECR (i.e., it's public)
    since that has no ARN.

    Examples
    --------
    >>> ecr_uri_to_repo_arn("012345678901.dkr.ecr.us-west-2.amazonaws.com/my-repo:latest")
    arn:aws:ecr:us-west-2:012345678901:repository/my-repo
    >>> ecr_uri_to_repo_arn("public.ecr.aws/amazonlinux/amazonlinux:latest")
    None
    """
    if "dkr" not in uri:
        return None

    tagless = uri.split(":")[0]
    dkr, repo = tagless.split("/")
    account_id, _, _, region, _, _ = dkr.split(".")
    return f"arn:aws:ecr:{region}:{account_id}:repository/{repo}"


class BatchJob(Construct):
    """An AWS Batch job running a Docker container"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        container_ecr_uri: str,
        vcpu: int,
        memory_mb: int,
        retry_attempts: int,
        log_group_name: str,
        secrets: None | dict[str, aws_batch.Secret] = None,
        stage: Literal["dev", "prod"],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.log_group = aws_logs.LogGroup(
            self,
            "JobLogGroup",
            log_group_name=log_group_name,
        )

        # Execution role needs ECR permissions to pull from private repo
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html#ecr-required-iam-permissions
        execution_role = aws_iam.Role(
            self,
            "ExecutionRole",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )
        execution_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
            )
        )
        if ecr_repo_arn := ecr_uri_to_repo_arn(container_ecr_uri):
            execution_role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    resources=[
                        ecr_repo_arn,
                    ],
                    actions=[
                        "ecr:BatchGetImage",
                        "ecr:GetDownloadUrlForLayer",
                    ],
                )
            )

        self.role = aws_iam.Role(
            self,
            "TaskRole",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"hls-vi-historical-processing-role-{stage}",
        )

        self.job_def = aws_batch.EcsJobDefinition(
            self,
            "JobDef",
            container=aws_batch.EcsEc2ContainerDefinition(
                self,
                "BatchContainerDef",
                image=aws_ecs.ContainerImage.from_registry(container_ecr_uri),
                execution_role=execution_role,
                job_role=self.role,
                cpu=vcpu,
                memory=Size.mebibytes(memory_mb),
                logging=aws_ecs.LogDriver.aws_logs(
                    stream_prefix="job",
                    log_group=self.log_group,
                ),
                secrets=secrets,
            ),
            timeout=Duration.hours(1),
            retry_attempts=retry_attempts,
            retry_strategies=[
                aws_batch.RetryStrategy.of(
                    aws_batch.Action.RETRY, aws_batch.Reason.CANNOT_PULL_CONTAINER
                ),
                aws_batch.RetryStrategy.of(
                    aws_batch.Action.RETRY, aws_batch.Reason.SPOT_INSTANCE_RECLAIMED
                ),
                aws_batch.RetryStrategy.of(
                    aws_batch.Action.EXIT,
                    aws_batch.Reason.custom(on_reason="*"),
                ),
            ],
            propagate_tags=True,
        )

        # It's useful to have the ARN of the job definition _without_ the revision
        # so submitted jobs use the "latest" active job
        self.job_def_arn_without_revision = ":".join(
            [
                "arn",
                "aws",
                "batch",
                Aws.REGION,
                Aws.ACCOUNT_ID,
                f"job-definition/{self.job_def.job_definition_name}",
            ]
        )
