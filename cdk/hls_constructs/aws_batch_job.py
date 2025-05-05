from typing import Any, Literal

from aws_cdk import Size, aws_batch, aws_ecs, aws_iam, aws_logs
from constructs import Construct


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
        stage: Literal["dev", "prod"],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.log_group = aws_logs.LogGroup(
            self,
            "JobLogGroup",
            log_group_name=log_group_name,
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
                cpu=vcpu,
                memory=Size.mebibytes(memory_mb),
                logging=aws_ecs.LogDriver.aws_logs(
                    stream_prefix="job",
                    log_group=self.log_group,
                ),
            ),
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
        )
