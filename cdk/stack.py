import os
from typing import Any

import jsii
from aws_cdk import (
    DockerVolume,
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
)
from constructs import Construct
from hls_constructs import AthenaLogsDatabase, BatchInfra, BatchJob
from settings import StackSettings

LAMBDA_EXCLUDE = [
    ".git*",
    "**/__pycache__",
    "**/*.egg-info",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "venv",
    ".venv",
    ".env*",
    "cdk",
    "cdk.out",
    "docs",
    "tests",
    "scripts",
]

# Mount root level `pyproject.toml` and `uv.lock` into container
UV_ASSET_REQUIREMENTS = "/asset-requirements"
UV_DOCKER_VOLUMES = [
    DockerVolume(
        host_path=os.path.abspath(file),
        container_path=f"{UV_ASSET_REQUIREMENTS}/{file}",
    )
    for file in (
        "pyproject.toml",
        "uv.lock",
    )
]


@jsii.implements(lambda_python.ICommandHooks)
class UvHooks:
    """Build hooks to setup UV and export a requirements.txt for building

    This will be unnecessary after UV support is built-in in aws-lambda-python-alpha,
    https://github.com/aws/aws-cdk/issues/31238
    """

    def __init__(self, groups: list[str] | None = None):
        super().__init__()
        self.groups = groups

    def after_bundling(self, input_dir: str, output_dir: str) -> list[str]:
        return []

    def before_bundling(self, input_dir: str, output_dir: str) -> list[str]:
        if self.groups:
            groups_arg = " ".join([f"--group {group}" for group in self.groups])
        else:
            groups_arg = " "

        return [
            f"python -m venv {input_dir}/uv_venv",
            f". {input_dir}/uv_venv/bin/activate",
            "pip install uv",
            "export UV_CACHE_DIR=/tmp",
            f"cd {UV_ASSET_REQUIREMENTS}",
            (
                f"uv export {groups_arg} --frozen --no-emit-project --no-dev "
                f"--no-default-groups --no-editable -o {input_dir}/requirements.txt"
            ),
            f"rm -rf {input_dir}/uv_venv",
        ]


class HlsViStack(Stack):
    """HLS-VI historical processing CDK stack."""

    def __init__(
        self, scope: Construct, stack_id: str, *, settings: StackSettings, **kwargs: Any
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # Apply IAM permission boundary to entire stack
        boundary = iam.ManagedPolicy.from_managed_policy_arn(
            self,
            "PermissionBoundary",
            settings.MCP_IAM_PERMISSION_BOUNDARY_ARN,
        )
        iam.PermissionsBoundary.of(self).apply(boundary)

        # ----------------------------------------------------------------------
        # Networking
        # ----------------------------------------------------------------------
        self.vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

        # ----------------------------------------------------------------------
        # Buckets
        # ----------------------------------------------------------------------
        self.lpdaac_protected_bucket = s3.Bucket.from_bucket_name(
            self,
            "LpdaacProtectedBucket",
            bucket_name=settings.LPDAAC_PROTECTED_BUCKET_NAME,
        )
        self.lpdaac_public_bucket = s3.Bucket.from_bucket_name(
            self,
            "LpdaacPublicBucket",
            bucket_name=settings.LPDAAC_PUBLIC_BUCKET_NAME,
        )

        self.output_bucket = s3.Bucket.from_bucket_name(
            self,
            "OutputBucket",
            bucket_name=settings.OUTPUT_BUCKET_NAME,
        )

        self.processing_bucket = s3.Bucket(
            self,
            "ProcessingBucket",
            bucket_name=settings.PROCESSING_BUCKET_NAME,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                # Setting expired_object_delete_marker cannot be done within a
                # lifecycle rule that also specifies expiration, expiration_date, or
                # tag_filters.
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        bucket_envvars = {
            "OUTPUT_BUCKET": settings.OUTPUT_BUCKET_NAME,
        }

        self.debug_bucket: s3.IBucket | None
        if settings.DEBUG_BUCKET_NAME:
            self.debug_bucket = s3.Bucket.from_bucket_name(
                self,
                "DebugBucket",
                bucket_name=settings.DEBUG_BUCKET_NAME,
            )
            bucket_envvars["DEBUG_BUCKET"] = settings.DEBUG_BUCKET_NAME
        else:
            self.debug_bucket = None

        # ----------------------------------------------------------------------
        # S3 processing bucket inventory to track our logs
        # ----------------------------------------------------------------------
        inventory_id = "granule_processing_logs"
        self.processing_bucket.add_inventory(
            enabled=True,
            destination=s3.InventoryDestination(
                bucket=s3.Bucket.from_bucket_name(
                    self, "ProcessingBucketRef", settings.PROCESSING_BUCKET_NAME
                ),
                prefix=settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX,
            ),
            inventory_id=inventory_id,
            format=s3.InventoryFormat.PARQUET,
            frequency=s3.InventoryFrequency.DAILY,
            objects_prefix=settings.PROCESSING_BUCKET_LOG_PREFIX,
            optional_fields=["LastModifiedDate"],
        )
        self.processing_bucket.add_lifecycle_rule(
            prefix=settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX,
            expiration=Duration.days(14),
        )

        # S3 service also needs permissions to push to the bucket
        self.processing_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                ],
                resources=[
                    self.processing_bucket.arn_for_objects(
                        f"{settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX}*"
                    ),
                ],
                principals=[
                    iam.ServicePrincipal("s3.amazonaws.com"),
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        logs_s3_inventory_location_s3path = "/".join(
            [
                f"s3://{settings.PROCESSING_BUCKET_NAME}",
                settings.PROCESSING_BUCKET_LOGS_INVENTORY_PREFIX.rstrip("/"),
                settings.PROCESSING_BUCKET_NAME,
                inventory_id,
                "hive",
            ]
        )

        self.athena_logs_database = AthenaLogsDatabase(
            self,
            "AthenaLogsDatabase",
            database_name=settings.ATHENA_LOGS_DATABASE_NAME,
            table_datetime_start=settings.ATHENA_LOGS_S3_INVENTORY_TABLE_START_DATETIME,
            logs_s3_inventory_location_s3path=logs_s3_inventory_location_s3path,
            logs_s3_inventory_table_name=settings.ATHENA_LOGS_S3_INVENTORY_TABLE_NAME,
            granule_processing_events_view_name=settings.ATHENA_LOGS_GRANULE_PROCESSING_EVENTS_VIEW_NAME,
        )

        # ----------------------------------------------------------------------
        # Earthdata Login (EDL) S3 credential rotator
        # ----------------------------------------------------------------------
        # NB - this secret must be created by developer team
        self.edl_user_pass_credentials = secretsmanager.Secret.from_secret_name_v2(
            self,
            id="EdlUserPassCredentials",
            secret_name=f"hls-vi-historical-orchestration/{settings.STAGE}/edl-user-credentials",
        )

        self.edl_s3_credentials = secretsmanager.Secret(
            self,
            id="EdlS3Credentials",
            secret_name=f"hls-vi-historical-orchestration/{settings.STAGE}/edl-s3-credentials",
            description="Temporary AWS credentials for accessing LPDAAC S3 buckets.",
        )

        # This Lambda sets the following keys in the `edl_s3_credentials` Secret,
        #   * ACCESS_KEY_ID
        #   * SECRET_ACCESS_KEY
        #   * SESSION_TOKEN
        self.edl_credential_rotator = lambda_python.PythonFunction(
            self,
            "EdlCredentialRotator",
            entry="src/edl_credential_rotator",
            index="handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "USER_PASS_SECRET_ID": self.edl_user_pass_credentials.secret_arn,
                "S3_CREDENTIALS_SECRET_ID": self.edl_s3_credentials.secret_arn,
            },
        )
        self.edl_user_pass_credentials.grant_read(self.edl_credential_rotator)
        self.edl_s3_credentials.grant_write(self.edl_credential_rotator)

        self.edl_credential_rotator_schedule = events.Rule(
            self,
            "EdlCredentialRotatorSchedule",
            schedule=events.Schedule.rate(
                Duration.minutes(30),
            ),
        )
        if settings.SCHEDULE_LPDAAC_CREDS_ROTATION:
            self.edl_credential_rotator_schedule.add_target(
                events_targets.LambdaFunction(
                    handler=self.edl_credential_rotator,
                )
            )

        # ----------------------------------------------------------------------
        # AWS Batch infrastructure
        # ----------------------------------------------------------------------
        self.batch_infra = BatchInfra(
            self,
            "HLS-VI-Infra",
            vpc=self.vpc,
            max_vcpu=settings.BATCH_MAX_VCPU,
            base_name=f"hls-vi-historical-orchestration-{settings.STAGE}",
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
            secrets={
                "LPDAAC_SECRET_ACCESS_KEY": batch.Secret.from_secrets_manager(
                    self.edl_s3_credentials, "SECRET_ACCESS_KEY"
                ),
                "LPDAAC_ACCESS_KEY_ID": batch.Secret.from_secrets_manager(
                    self.edl_s3_credentials, "ACCESS_KEY_ID"
                ),
                "LPDAAC_SESSION_TOKEN": batch.Secret.from_secrets_manager(
                    self.edl_s3_credentials, "SESSION_TOKEN"
                ),
            },
            stage=settings.STAGE,
        )
        self.processing_bucket.grant_read_write(self.processing_job.role)
        self.output_bucket.grant_read_write(self.processing_job.role)
        self.lpdaac_protected_bucket.grant_read(self.processing_job.role)
        self.lpdaac_public_bucket.grant_read(self.processing_job.role)
        self.edl_s3_credentials.grant_read(self.processing_job.role)
        if self.debug_bucket is not None:
            self.debug_bucket.grant_read(self.processing_job.role)

        # ----------------------------------------------------------------------
        # One-off inventory conversion Lambda
        # ----------------------------------------------------------------------
        self.inventory_converter_lambda = lambda_python.PythonFunction(
            self,
            "InventoryConverterHandler",
            entry="src/",
            index="inventory_converter/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=1024,
            timeout=Duration.minutes(10),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_INVENTORY_PREFIX": settings.PROCESSING_BUCKET_INVENTORY_PREFIX,
            },
            bundling=lambda_python.BundlingOptions(
                command_hooks=UvHooks(groups=["arrow"]),
                asset_excludes=LAMBDA_EXCLUDE,
                volumes=UV_DOCKER_VOLUMES,
            ),
            ephemeral_storage_size=Size.mebibytes(2500),
        )
        self.processing_bucket.grant_read_write(
            self.inventory_converter_lambda,
            objects_key_pattern=f"{settings.PROCESSING_BUCKET_INVENTORY_PREFIX}*",
        )

        # ----------------------------------------------------------------------
        # Queue feeder
        # ----------------------------------------------------------------------
        self.queue_feeder_lambda = lambda_python.PythonFunction(
            self,
            "QueueFeederHandler",
            entry="src/",
            index="queue_feeder/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=Duration.minutes(10),
            reserved_concurrent_executions=1,
            environment={
                "FEEDER_MAX_ACTIVE_JOBS": str(settings.FEEDER_MAX_ACTIVE_JOBS),
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_INVENTORY_PREFIX": settings.PROCESSING_BUCKET_INVENTORY_PREFIX,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "BATCH_JOB_DEFINITION_NAME": self.processing_job.job_def.job_definition_name,
                **bucket_envvars,
            },
            bundling=lambda_python.BundlingOptions(
                command_hooks=UvHooks(groups=["arrow"]),
                asset_excludes=LAMBDA_EXCLUDE,
                volumes=UV_DOCKER_VOLUMES,
            ),
        )

        self.processing_bucket.grant_read_write(
            self.queue_feeder_lambda,
        )

        # Ref: AWS Batch IAM actions/resources/conditions
        # https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsbatch.html
        self.queue_feeder_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    self.batch_infra.queue.job_queue_arn,
                    self.processing_job.job_def_arn_without_revision,
                ],
                actions=[
                    "batch:SubmitJob",
                ],
            )
        )
        self.queue_feeder_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "batch:ListJobs",
                ],
            )
        )

        # Schedule queue feeder
        self.queue_feeder_schedule = events.Rule(
            self,
            "QueueFeederSchedule",
            schedule=events.Schedule.rate(
                Duration.minutes(settings.FEEDER_EXECUTION_SCHEDULE_RATE_MINUTES),
            ),
        )
        if settings.SCHEDULE_QUEUE_FEEDER:
            self.queue_feeder_schedule.add_target(
                events_targets.LambdaFunction(
                    event=events.RuleTargetInput.from_object(
                        {
                            "granule_submit_count": settings.FEEDER_GRANULE_SUBMIT_COUNT,
                        }
                    ),
                    handler=self.queue_feeder_lambda,
                    retry_attempts=3,
                )
            )

        # ----------------------------------------------------------------------
        # Job monitor & retry system
        # ----------------------------------------------------------------------
        # Queue for job failures we need to investigate (bugs, repeat errors, etc)
        self.job_failure_dlq = sqs.Queue(
            self,
            "JobRetryFailureDLQ",
            queue_name=settings.JOB_FAILURE_DLQ_NAME,
            retention_period=Duration.days(14),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # Queue for failed AWS Batch processing jobs we want to requeue
        self.job_retry_queue = sqs.Queue(
            self,
            "JobRetryFailureQueue",
            queue_name=settings.JOB_RETRY_QUEUE_NAME,
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.job_failure_dlq,
                # Route to DLQ immediately if we can't process
                max_receive_count=1,
            ),
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(2),
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        self.job_monitor_lambda = lambda_python.PythonFunction(
            self,
            "JobMonitorHandler",
            entry="src/",
            index="job_monitor/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "PROCESSING_BUCKET_NAME": self.processing_bucket.bucket_name,
                "PROCESSING_BUCKET_LOG_PREFIX": settings.PROCESSING_BUCKET_LOG_PREFIX,
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "JOB_RETRY_QUEUE_URL": self.job_retry_queue.queue_url,
                "JOB_FAILURE_DLQ_URL": self.job_failure_dlq.queue_url,
                "PROCESSING_JOB_RETRY_ATTEMPTS": str(
                    settings.PROCESSING_JOB_RETRY_ATTEMPTS
                ),
            },
            bundling=lambda_python.BundlingOptions(
                command_hooks=UvHooks(),
                asset_excludes=LAMBDA_EXCLUDE,
                volumes=UV_DOCKER_VOLUMES,
            ),
        )

        self.processing_bucket.grant_read_write(
            self.job_monitor_lambda,
        )
        self.job_retry_queue.grant_send_messages(self.job_monitor_lambda)
        self.job_failure_dlq.grant_send_messages(self.job_monitor_lambda)

        # Events from AWS Batch "job state change events" in our processing queue
        # Ref: https://docs.aws.amazon.com/batch/latest/userguide/batch_job_events.html
        self.processing_job_events_rule = events.Rule(
            self,
            "ProcessingJobEventsRule",
            event_pattern=events.EventPattern(
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
                    "status": ["FAILED", "SUCCEEDED"],
                },
            ),
            targets=[
                events_targets.LambdaFunction(
                    handler=self.job_monitor_lambda,
                    retry_attempts=3,
                )
            ],
        )

        # ----------------------------------------------------------------------
        # Requeuer
        # ----------------------------------------------------------------------
        self.job_requeuer_lambda = lambda_python.PythonFunction(
            self,
            "JobRequeuerHandler",
            entry="src/",
            index="job_requeuer/handler.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                "BATCH_QUEUE_NAME": self.batch_infra.queue.job_queue_name,
                "BATCH_JOB_DEFINITION_NAME": self.processing_job.job_def.job_definition_name,
                **bucket_envvars,
            },
            bundling=lambda_python.BundlingOptions(
                command_hooks=UvHooks(),
                asset_excludes=LAMBDA_EXCLUDE,
                volumes=UV_DOCKER_VOLUMES,
            ),
        )

        self.processing_bucket.grant_read_write(
            self.job_requeuer_lambda, objects_key_pattern="logs/*"
        )
        self.job_requeuer_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    self.batch_infra.queue.job_queue_arn,
                    self.processing_job.job_def_arn_without_revision,
                ],
                actions=[
                    "batch:SubmitJob",
                ],
            )
        )
        self.job_retry_queue.grant_consume_messages(self.job_requeuer_lambda)

        # Requeuer consumes from queue that the "job monitor" publishes to
        self.job_requeuer_lambda.add_event_source_mapping(
            "JobRequeuerRetryQueueTrigger",
            batch_size=100,
            max_batching_window=Duration.minutes(1),
            report_batch_item_failures=True,
            event_source_arn=self.job_retry_queue.queue_arn,
        )
