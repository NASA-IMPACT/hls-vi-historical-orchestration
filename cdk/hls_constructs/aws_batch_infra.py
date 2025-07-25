from typing import Any

from aws_cdk import CfnOutput, aws_batch as batch, aws_ec2 as ec2
from constructs import Construct


class BatchInfra(Construct):
    """AWS Batch compute environment and queues."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        max_vcpu: int,
        base_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ecs_machine_image = batch.EcsMachineImage(
            image=ec2.MachineImage.from_ssm_parameter("/mcp/amis/aml2-ecs"),
            image_type=batch.EcsMachineImageType.ECS_AL2,
        )

        self.compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            "ComputeEnvironment",
            allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            images=[ecs_machine_image],
            spot=True,
            minv_cpus=0,
            maxv_cpus=max_vcpu,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            vpc=vpc,
            compute_environment_name=f"{base_name}-compute-environment",
        )

        self.queue = batch.JobQueue(
            self,
            "JobQueue",
            job_queue_name=f"{base_name}-job-queue",
        )
        self.queue.add_compute_environment(self.compute_environment, 1)

        CfnOutput(
            self,
            "JobQueueName",
            value=self.queue.job_queue_name,
        )
