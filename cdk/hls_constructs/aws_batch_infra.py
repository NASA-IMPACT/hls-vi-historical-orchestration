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
        instance_types: list[str] | None,
        max_vcpu: int,
        base_name: str,
        **kwargs: Any,
    ) -> None:
        """Setup an AWS Batch ComputeEnvironment and JobQueue

        Parameters
        ----------
        vpc
            VPC in which the ComputeEnvironment will launch Instances.
        instance_types
            If provided, limit ComputeEnvironment to these instance types. Instance
            types can be provided by string value and will be converted into the
            appropriate enum (e.g., r4.large). If not provided AWS Batch will use
            "optimal" instance types.
        max_vcpu
            Maximum number of CPUs in the ComputeEnvironment
        base_name
            Prefix for naming the ComputeEnvironment and JobQueue resources.
        """
        super().__init__(scope, construct_id, **kwargs)

        ecs_machine_image = batch.EcsMachineImage(
            image=ec2.MachineImage.from_ssm_parameter("/mcp/amis/aml2-ecs"),
            image_type=batch.EcsMachineImageType.ECS_AL2,
        )

        if instance_types:
            ec2_instance_types = [
                ec2.InstanceType(instance_type) for instance_type in instance_types
            ]
        else:
            ec2_instance_types = None

        self.compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            "ComputeEnvironment",
            allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            images=[ecs_machine_image],
            instance_types=ec2_instance_types,
            use_optimal_instance_classes=ec2_instance_types is None,
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
