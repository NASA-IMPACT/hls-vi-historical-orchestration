from aws_cdk import CfnOutput, aws_batch, aws_ec2
from constructs import Construct


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: aws_ec2.IVpc,
        max_vcpu: int,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        ecs_machine_image = aws_batch.EcsMachineImage(
            image=aws_ec2.MachineImage.from_ssm_parameter("/mcp/amis/aml2-ecs"),
            image_type=aws_batch.EcsMachineImageType.ECS_AL2,
        )

        self.compute_environment = aws_batch.ManagedEc2EcsComputeEnvironment(
            self,
            "ComputeEnvironment",
            allocation_strategy=aws_batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            images=[ecs_machine_image],
            spot=True,
            minv_cpus=0,
            maxv_cpus=max_vcpu,
            vpc_subnets=aws_ec2.SubnetSelection(
                subnet_type=aws_ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            vpc=vpc,
        )

        self.queue = aws_batch.JobQueue(
            self,
            "JobQueue",
        )
        self.queue.add_compute_environment(self.compute_environment, 1)

        CfnOutput(
            self,
            "JobQueueName",
            value=self.queue.job_queue_name,
        )
