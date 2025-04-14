from aws_cdk import Size, aws_batch, aws_ecs, aws_iam
from constructs import Construct


class BatchJob(Construct):
    """An AWS Batch job running a Docker container"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        container_ecr_uri: str,
        vcpu: int,
        memory_mb: int,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.role = aws_iam.Role(
            self,
            "TaskRole",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
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
            ),
            retry_attempts=3,
            retry_strategies=[
                aws_batch.RetryStrategy.of(
                    aws_batch.Action.EXIT, aws_batch.Reason.CANNOT_PULL_CONTAINER
                ),
                aws_batch.RetryStrategy.of(
                    aws_batch.Action.EXIT, aws_batch.Reason.SPOT_INSTANCE_RECLAIMED
                ),
            ],
        )
