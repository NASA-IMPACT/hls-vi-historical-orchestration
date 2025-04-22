import os
from typing import Any

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2,
    aws_events,
    aws_events_targets,
    aws_iam,
    aws_lambda,
    aws_s3,
    aws_sqs,
)
from constructs import Construct

from hls_constructs import BatchInfra, BatchJob
from settings import StackSettings

LAMBDA_EXCLUDE = [
    "**/*.egg-info",
]


class HlsViStack(Stack):
    """HLS-VI historical processing CDK stack."""

    def __init__(
        self, scope: Construct, stack_id: str, *, settings: StackSettings, **kwargs: Any
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # Apply IAM permission boundary to entire stack
        boundary = aws_iam.ManagedPolicy.from_managed_policy_arn(
            self,
            "PermissionBoundary",
            settings.MCP_IAM_PERMISSION_BOUNDARY_ARN,
        )
        aws_iam.PermissionsBoundary.of(self).apply(boundary)

        # ----------------------------------------------------------------------
        # Networking
        # ----------------------------------------------------------------------
        self.vpc = aws_ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

        # ----------------------------------------------------------------------
        # Buckets
        # ----------------------------------------------------------------------
        self.lpdaac_private_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "LpdaacPrivateBucket",
            bucket_name=settings.LPDAAC_PRIVATE_BUCKET_NAME,
        )
        self.lpdaac_public_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "LpdaacPublicBucket",
            bucket_name=settings.LPDAAC_PUBLIC_BUCKET_NAME,
        )

        self.output_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "OutputBucket",
            bucket_name=settings.OUTPUT_BUCKET_NAME,
        )

        self.processing_bucket = aws_s3.Bucket(
            self,
            "ProcessingBucket",
            bucket_name=settings.PROCESSING_BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                # Setting expired_object_delete_marker cannot be done within a
                # lifecycle rule that also specifies expiration, expiration_date, or
                # tag_filters.
                aws_s3.LifecycleRule(expired_object_delete_marker=True),
                aws_s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
        )

        # ----------------------------------------------------------------------
        # AWS Batch infrastructure
        # ----------------------------------------------------------------------
        self.batch_infra = BatchInfra(
            self,
            "HLS-VI-Infra",
            vpc=self.vpc,
            max_vcpu=settings.BATCH_MAX_VCPU,
        )

        # ----------------------------------------------------------------------
        # HLS-VI processing compute job
        # ----------------------------------------------------------------------
        self.processing_job = BatchJob(
            self,
            "HLS-VI-Processing",
            container_ecr_uri=settings.PROCESSING_CONTAINER_ECR_URI,
            vcpu=settings.PROCESSING_JOB_VCPU,
            memory_mb=settings.PROCESSING_JOB_MEMORY_MB,
            retry_attempts=settings.PROCESSING_JOB_RETRY_ATTEMPTS,
            log_group_name=settings.PROCESSING_LOG_GROUP_NAME,
            stage=settings.STAGE,
        )
        self.processing_bucket.grant_read_write(self.processing_job.role)
        self.output_bucket.grant_read_write(self.processing_job.role)
        self.lpdaac_private_bucket.grant_read(self.processing_job.role)
        self.lpdaac_public_bucket.grant_read(self.processing_job.role)

        # ----------------------------------------------------------------------
        # Queue feeder
        # ----------------------------------------------------------------------
        self.queue_feeder_lambda = aws_lambda.Function(
            self,
            "QueueFeederHandler",
            code=aws_lambda.Code.from_asset(
                os.path.join("lambdas"),
                exclude=LAMBDA_EXCLUDE,
            ),
            handler="queue_feeder.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=Duration.minutes(10),
            reserved_concurrent_executions=1,
            environment={
                "PROCESSING_BUCKET": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_INVENTORY_PREFIX": settings.PROCESSING_BUCKET_INVENTORY_PREFIX,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
            },
        )

        self.processing_bucket.grant_read_write(
            self.queue_feeder_lambda,
            objects_key_pattern=f"{settings.PROCESSING_BUCKET_INVENTORY_PREFIX}/*",
        )

        self.queue_feeder_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                resources=[
                    self.batch_infra.queue.job_queue_arn,
                    self.processing_job.job_def.job_definition_arn,
                ],
                actions=[
                    "batch:ListJobs",
                    "batch:SubmitJob",
                ],
            )
        )

        # Schedule queue feeder
        self.queue_feeder_schedule = aws_events.Rule(
            self,
            "QueueFeederSchedule",
            schedule=aws_events.Schedule.rate(
                Duration.minutes(settings.FEEDER_EXECUTION_SCHEDULE_RATE_MINUTES),
            ),
            targets=[
                aws_events_targets.LambdaFunction(
                    handler=self.queue_feeder_lambda,
                    retry_attempts=3,
                )
            ],
        )

        # ----------------------------------------------------------------------
        # Job monitor & retry system
        # ----------------------------------------------------------------------

        # Queue for failed AWS Batch processing jobs
        self.job_retry_failure_queue = aws_sqs.Queue(
            self,
            "JobRetryFailureQueue",
            queue_name=settings.JOB_RETRY_FAILURE_QUEUE_NAME,
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(2),
            enforce_ssl=True,
            encryption=aws_sqs.QueueEncryption.SQS_MANAGED,
        )

        self.job_monitor_lambda = aws_lambda.Function(
            self,
            "JobMonitorHandler",
            code=aws_lambda.Code.from_asset(
                os.path.join("lambdas"),
                exclude=LAMBDA_EXCLUDE,
            ),
            handler="job_monitor.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "PROCESSING_BUCKET": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_FAILURE_PREFIX": settings.PROCESSING_BUCKET_FAILURE_PREFIX,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "JOB_RETRY_FAILURE_QUEUE_NAME": self.job_retry_failure_queue.queue_name,
                "PROCESSING_JOB_RETRY_ATTEMPTS": str(
                    settings.PROCESSING_JOB_RETRY_ATTEMPTS
                ),
            },
        )

        self.processing_bucket.grant_read_write(
            self.queue_feeder_lambda,
            objects_key_pattern=f"{settings.PROCESSING_BUCKET_FAILURE_PREFIX}/*",
        )
        self.job_retry_failure_queue.grant_send_messages(self.job_monitor_lambda)

        # Events from AWS Batch "job state change events" in our processing queue
        # Ref: https://docs.aws.amazon.com/batch/latest/userguide/batch_job_events.html
        self.processing_job_events_rule = aws_events.Rule(
            self,
            "ProcessingJobEventsRule",
            event_pattern=aws_events.EventPattern(
                source=["aws.batch"],
                detail={
                    # only retry jobs from our queue and job definition that failed
                    # on their last attempt
                    "jobQueue": [self.batch_infra.queue.job_queue_arn],
                    "jobDefinition": [
                        {
                            "wildcard": f"*{self.processing_job.job_def.job_definition_name}*"
                        },
                    ],
                    "status": ["FAILED"],
                },
            ),
            targets=[
                aws_events_targets.LambdaFunction(
                    handler=self.job_monitor_lambda,
                    retry_attempts=3,
                )
            ],
        )

        # ----------------------------------------------------------------------
        # Requeuer
        # ----------------------------------------------------------------------
        self.job_requeuer_lambda = aws_lambda.Function(
            self,
            "JobRequeuerHandler",
            code=aws_lambda.Code.from_asset(
                os.path.join("lambdas"),
                exclude=LAMBDA_EXCLUDE,
            ),
            handler="job_requeuer.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "PROCESSING_BUCKET": self.processing_bucket.bucket_name,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "JOB_RETRY_FAILURE_QUEUE_NAME": self.job_retry_failure_queue.queue_name,
            },
        )

        self.processing_bucket.grant_read_write(
            self.job_requeuer_lambda, objects_key_pattern="logs/*"
        )
        self.processing_job.job_def.grant_submit_job(
            self.job_requeuer_lambda, self.batch_infra.queue
        )
        self.job_retry_failure_queue.grant_consume_messages(self.job_requeuer_lambda)

        # Requeuer consumes from queue that the "job monitor" publishes to
        self.job_requeuer_lambda.add_event_source_mapping(
            "JobRequeuerRetryQueueTrigger",
            batch_size=100,
            max_batching_window=Duration.minutes(1),
            report_batch_item_failures=True,
            event_source_arn=self.job_retry_failure_queue.queue_arn,
        )
