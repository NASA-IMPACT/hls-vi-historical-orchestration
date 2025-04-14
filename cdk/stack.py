from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2,
    aws_iam,
    aws_s3,
)
from constructs import Construct

from hls_constructs import BatchJob
from settings import StackSettings


class HlsViStack(Stack):
    """HLS-VI historical processing CDK stack."""

    def __init__(
        self, scope: Construct, id: str, settings: StackSettings, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Apply IAM permission boundary to entire stack
        boundary = aws_iam.ManagedPolicy.from_managed_policy_arn(
            self,
            "PermissionBoundary",
            settings.MCP_IAM_PERMISSION_BOUNDARY_ARN,
        )
        aws_iam.PermissionsBoundary.of(self).apply(boundary)

        self.vpc = aws_ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

        self.lpdaac_granule_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "LpdaacGranuleBucket",
            bucket_name=settings.LPDAAC_GRANULE_BUCKET_NAME,
        )
        self.lpdaac_metadata_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "LpdaacMetadataBucket",
            bucket_name=settings.LPDAAC_METADATA_BUCKET_NAME,
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
                    expiration=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
        )

        self.output_bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "OutputBucket",
            bucket_name=settings.OUTPUT_BUCKET_NAME,
        )

        # AWS Batch processing job container
        self.processing_job = BatchJob(self, "ProcessingJob")
        self.processing_bucket.grant_read_write(self.processing_job.role)
        self.output_bucket.grant_read_write(self.processing_job.role)
        self.lpdaac_granule_bucket.grant_read(self.processing_job.role)
        self.lpdaac_metadata_bucket.grant_read(self.processing_job.role)
