from aws_cdk import aws_iam
from constructs import Construct


class BatchJob(Construct):
    """An AWS Batch job running a Docker container"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.role = aws_iam.Role(
            self,
            "TaskRole",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # TODO: finish AWS Batch JobDefinition setup
