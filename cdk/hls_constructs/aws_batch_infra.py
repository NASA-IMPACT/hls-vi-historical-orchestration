from constructs import Construct


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""
    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)
        # TODO: finish AWS Batch infra setup
