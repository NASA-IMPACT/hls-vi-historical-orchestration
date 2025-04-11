from aws_cdk import aws_iam
from constructs import Construct


class BatchInfra(Construct):

    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)
        
        # TODO: finish AWS Batch infra setup
