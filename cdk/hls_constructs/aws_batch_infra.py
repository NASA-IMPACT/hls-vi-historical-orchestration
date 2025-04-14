from aws_cdk import aws_ec2
from constructs import Construct


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: aws_ec2.Vpc,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)
