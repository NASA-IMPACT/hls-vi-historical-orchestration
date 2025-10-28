import base64
import datetime as dt
import json
from typing import Any

from aws_cdk import (
    Aws,
    RemovalPolicy,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
)
from constructs import Construct

# Obtained from running AWS Glue crawler to infer the schema
ATHENA_STRUCT_AWS_BATCH_JOB_INFO = "struct<jobArn:string,jobName:string,jobId:string,jobQueue:string,status:string,attempts:array<struct<container:struct<containerInstanceArn:string,taskArn:string,exitCode:int,logStreamName:string,networkInterfaces:array<string>>,startedAt:bigint,stoppedAt:bigint,statusReason:string>>,statusReason:string,createdAt:bigint,retryStrategy:struct<attempts:int,evaluateOnExit:array<struct<onReason:string,action:string,onStatusReason:string>>>,startedAt:bigint,stoppedAt:bigint,dependsOn:array<string>,jobDefinition:string,parameters:string,container:struct<image:string,command:array<string>,jobRoleArn:string,executionRoleArn:string,volumes:array<string>,environment:array<struct<name:string,value:string>>,mountPoints:array<string>,readonlyRootFilesystem:boolean,ulimits:array<string>,exitCode:int,containerInstanceArn:string,taskArn:string,logStreamName:string,networkInterfaces:array<string>,resourceRequirements:array<struct<value:string,type:string>>,logConfiguration:struct<logDriver:string,options:struct<awslogs-group:string,awslogs-region:string,awslogs-stream-prefix:string>,secretOptions:array<string>>,secrets:array<string>>,timeout:struct<attemptDurationSeconds:int>,tags:struct<resourceArn:string>,propagateTags:boolean,platformCapabilities:array<string>,eksAttempts:array<string>,isTerminated:boolean>"


def athena_type_to_presto(athena_type: str | None) -> str:
    """Convert Athena column types to Presto types

    This only supports a relevant subset of types. See also:
    https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    """
    # for type checking...
    if athena_type is None:
        raise ValueError("Cannot parse null athena type")

    lowered = athena_type.lower()
    return {
        "string": "varchar",
        "struct": "row",
        "float": "real",
        "binary": "varbinary",
    }.get(lowered, lowered)


class AthenaLogsDatabase(Construct):
    """Athena table for querying granule processing event logs"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database_name: str,
        logs_bucket: s3.IBucket,
        logs_s3_prefix: str,
        logs_event_queue_name: str,
        table_datetime_start: dt.datetime,
        logs_s3_inventory_location_s3path: str,
        logs_s3_inventory_table_name: str,
        granule_processing_events_view_name: str,
        work_group_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = glue.CfnDatabase(
            self,
            "Database",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Database for HLS-VI Historical Orchestration task logging.",
            ),
        )

        self.s3_inventory_table = self._create_s3_inventory_table(
            database=self.database,
            table_datetime_start=table_datetime_start,
            logs_s3_inventory_table_name=logs_s3_inventory_table_name,
            logs_s3_inventory_location_s3path=logs_s3_inventory_location_s3path,
        )
        self.granule_processing_events_view = (
            self._create_granule_processing_events_view(
                database=self.database,
                logs_s3_inventory_table_name=logs_s3_inventory_table_name,
                logs_s3_inventory_table=self.s3_inventory_table,
                granule_processing_events_view_name=granule_processing_events_view_name,
            )
        )

        # self.granule_processing_events_raw_table = self._create_granule_processing_events_raw_logs_table(
        #     database_arn=f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{database_name}",
        #     logs_bucket=logs_bucket,
        #     logs_s3_prefix=logs_s3_prefix,
        # )

        self._create_granule_processing_events_raw_logs_crawler(
            database_name=database_name,
            logs_bucket=logs_bucket,
            logs_s3_prefix=logs_s3_prefix,
            logs_event_queue_name=logs_event_queue_name,
        )

    def _create_s3_inventory_table(
        self,
        *,
        database: glue.CfnDatabase,
        table_datetime_start: dt.datetime,
        logs_s3_inventory_table_name: str,
        logs_s3_inventory_location_s3path: str,
    ) -> glue.CfnTable:
        """Create a Glue table for the S3 inventory reports"""
        database_name = database.database_name
        assert database_name is not None

        table = glue.CfnTable(
            self,
            "S3InventoryTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=logs_s3_inventory_table_name,
                table_type="EXTERNAL_TABLE",
                parameters={
                    "projection.dt.range": f"{table_datetime_start:%Y-%m-%d-%H-%M},NOW",
                    "projection.dt.interval.unit": "HOURS",
                    "EXTERNAL": "TRUE",
                    "projection.dt.type": "date",
                    "projection.enabled": "true",
                    "projection.dt.interval": "1",
                    "projection.dt.format": "yyyy-MM-dd-HH-mm",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        comment="Report date",
                        type="String",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                            name="bucket",
                            comment="The name of the bucket that the inventory is for.",
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="key",
                            comment="The object key name (or key) that uniquely identifies the object in the bucket.",
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="version_id",
                            comment="The object version ID.",
                            type="String",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="is_latest",
                            comment="Set to True if the object is the current version of the object.",
                            type="boolean",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="is_delete_marker",
                            comment="Set to True if the object is a delete marker.",
                            type="boolean",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="last_modified_date",
                            comment="The object creation date or the last modified date, whichever is the latest.",
                            type="timestamp",
                        ),
                    ],
                    location=logs_s3_inventory_location_s3path,
                    input_format="org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={
                            "serialization.format": "1",
                        },
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_dependency(database)
        return table

    def _create_granule_processing_events_view(
        self,
        *,
        database: glue.CfnDatabase,
        logs_s3_inventory_table_name: str,
        logs_s3_inventory_table: glue.CfnTable,
        granule_processing_events_view_name: str,
    ) -> glue.CfnTable:
        """Create a view for granule processing events logs"""
        database_name = database.database_name
        assert database_name is not None

        sql = rf"""
        SELECT
            regexp_extract(key, 'outcome=(\w+)', 1) as outcome,
            regexp_extract(key, 'platform=(\w+)', 1) as platform,
            date_parse(regexp_extract(key, 'acquisition_date=([\d-]+)', 1), '%Y-%m-%d') AS acquisition_date,
            regexp_extract(key, 'granule_id=([\w\.]+)', 1) AS granule_id,
            cast(regexp_extract(key, 'attempt=([0-9]+)', 1) AS INT) AS attempt,
            last_modified_date,
            key
        FROM {logs_s3_inventory_table_name}
        WHERE
            dt = (SELECT max(dt) FROM {logs_s3_inventory_table_name})
            AND is_latest
            AND NOT is_delete_marker
        """

        columns = [
            glue.CfnTable.ColumnProperty(
                name="outcome",
                comment="The processing event outcome (failed, success).",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="platform",
                comment="The platform (L30, S30).",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="acquisition_date",
                comment="The acquisition date.",
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="granule_id",
                comment="HLS granule ID.",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="attempt",
                comment="Attempt.",
                type="int",
            ),
            glue.CfnTable.ColumnProperty(
                name="last_modified_date",
                comment="The event log's creation date or the last modified date, whichever is the latest.",
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="key",
                comment="The event log S3 key.",
                type="String",
            ),
        ]

        view_specification = {
            "originalSql": sql,
            "catalog": Aws.ACCOUNT_ID,
            "schema": database_name,
            "columns": [
                {"name": column.name, "type": athena_type_to_presto(column.type)}
                for column in columns
            ],
        }

        sql_b64 = base64.b64encode(
            json.dumps(view_specification).encode("utf-8")
        ).decode("utf-8")

        view = glue.CfnTable(
            self,
            "GranuleProcessingEventsView",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=granule_processing_events_view_name,
                table_type="VIRTUAL_VIEW",
                parameters={"presto_view": "true", "comment": "Presto View"},
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="dt",
                        comment="Report date",
                        type="String",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    input_format="org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                ),
                view_original_text=f"/* Presto View: {sql_b64} */",
                view_expanded_text="/* Presto View */",
            ),
        )
        view.apply_removal_policy(RemovalPolicy.DESTROY)
        view.add_dependency(logs_s3_inventory_table)
        return view

    #    def _create_granule_processing_events_raw_logs_table(
    #        self,
    #        *,
    #        database_arn: str,
    #        logs_bucket: s3.IBucket,
    #        logs_s3_prefix: str,
    #    ) -> glue_alpha.S3Table:
    #        """Create raw logs Glue table"""
    #        table = glue_alpha.S3Table(
    #            self,
    #            "GranuleProcessingEventsRawLogsTable",
    #            database=glue_alpha.Database.from_database_arn(
    #                self, "DatabaseLookup", database_arn=database_arn
    #            ),
    #            table_name="granule-processing-events-raw",
    #            data_format=glue_alpha.DataFormat.JSON,
    #            columns=[
    #                glue_alpha.Column(
    #                    name="attempt",
    #                    comment="Job attempt",
    #                    type=glue_alpha.Schema.INTEGER,
    #                ),
    #                glue_alpha.Column(
    #                    name="job_info",
    #                    comment="AWS Batch job information",
    #                    type=glue_alpha.Type(
    #                        input_string=ATHENA_STRUCT_AWS_BATCH_JOB_INFO,
    #                        is_primitive=False,
    #                    ),
    #                ),
    #            ],
    #            partition_keys=[
    #                glue_alpha.Column(
    #                    name="outcome",
    #                    comment="Job outcome",
    #                    type=glue_alpha.Schema.STRING,
    #                ),
    #                glue_alpha.Column(
    #                    name="platform",
    #                    comment="Satellite platform",
    #                    type=glue_alpha.Schema.STRING,
    #                ),
    #                glue_alpha.Column(
    #                    name="acquisition_date",
    #                    comment="Granule acquisition date",
    #                    type=glue_alpha.Schema.STRING,
    #                ),
    #                glue_alpha.Column(
    #                    name="granule_id",
    #                    comment="Granule ID",
    #                    type=glue_alpha.Schema.STRING,
    #                ),
    #            ],
    #            storage_parameters=[
    #                glue_alpha.StorageParameter.custom(
    #                    "serde.serialization.paths", "attempt,granule_id,job_info,outcome"
    #                )
    #            ],
    #            bucket=s3.Bucket.from_bucket_arn(
    #                self, "LogsBucketRef", bucket_arn=logs_bucket.bucket_arn
    #            ),
    #            s3_prefix=logs_s3_prefix,
    #            enable_partition_filtering=True,
    #        )
    #        table.node.default_child.add_property_override(
    #            "TableInput.StorageDescriptor.SerdeInfo.Parameters",
    #            {
    #                "paths": "attempt,granule_id,job_info,outcome",
    #            },
    #        )
    #        return table

    #    def _create_granule_processing_events_raw_logs_table(
    #        self,
    #        *,
    #        database_name: str,
    #        logs_bucket: s3.IBucket,
    #        logs_s3_prefix: str,
    #    ) -> glue.CfnTable:
    #        """Create raw logs Glue table"""
    #        table = glue.CfnTable(
    #            self,
    #            "GranuleProcessingEventsRawLogsTable",
    #            catalog_id=Aws.ACCOUNT_ID,
    #            database_name=database_name,
    #            table_input=glue.CfnTable.TableInputProperty(
    #                name="granule-processing-event-raw-logs",
    #                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
    #                    columns=[
    #                        glue.CfnTable.ColumnProperty(
    #                            name="attempt",
    #                            comment="Job attempt",
    #                            type="int",
    #                        ),
    #                        glue.CfnTable.ColumnProperty(
    #                            name="job_info",
    #                            comment="AWS Batch job information",
    #                            type=ATHENA_STRUCT_AWS_BATCH_JOB_INFO,
    #                        ),
    #                    ],
    #                    location=logs_bucket.s3_url_for_object(key=logs_s3_prefix),
    #                    compressed=False,
    #                    number_of_buckets=-1,
    #                    serde_info=glue.CfnTable.SerdeInfoProperty(
    #                        parameters={
    #                            "paths": "attempt,granule_id,job_info,outcome",
    #                        },
    #                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
    #                    ),
    #                    parameters={
    #                        "classification": "json",
    #                        "partition_filtering.enabled": "true",
    #                    }
    #                ),
    #                partition_keys=[
    #                    glue.CfnTable.ColumnProperty(
    #                        name="outcome",
    #                        comment="Job outcome",
    #                        type="string",
    #                    ),
    #                    glue.CfnTable.ColumnProperty(
    #                        name="platform",
    #                        comment="Satellite platform",
    #                        type="string",
    #                    ),
    #                    glue.CfnTable.ColumnProperty(
    #                        name="acquisition_date",
    #                        comment="Granule acquisition date",
    #                        type="string",
    #                    ),
    #                    glue.CfnTable.ColumnProperty(
    #                        name="granule_id",
    #                        comment="Granule ID",
    #                        type="string",
    #                    ),
    #                ],
    #                table_type="EXTERNAL_TABLE",
    #            ),
    #        )
    #
    #        return table

    def _create_granule_processing_events_raw_logs_crawler(
        self,
        *,
        database_name: str,
        logs_bucket: s3.IBucket,
        logs_s3_prefix: str,
        logs_event_queue_name: str,
    ) -> tuple[sqs.Queue, iam.Role, glue.CfnCrawler]:
        """Create raw logs Glue table and event driven Crawler"""
        # Create queue to receive S3 ObjectCreated and ObjectDeleted notifications
        self.logs_event_queue = sqs.Queue(
            self,
            "LogsS3EventQueue",
            queue_name=logs_event_queue_name,
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )
        logs_bucket.add_object_created_notification(
            s3n.SqsDestination(self.logs_event_queue),
            s3.NotificationKeyFilter(
                prefix=f"{logs_s3_prefix}*",
            ),
        )
        logs_bucket.add_object_removed_notification(
            s3n.SqsDestination(self.logs_event_queue),
            s3.NotificationKeyFilter(
                prefix=f"{logs_s3_prefix}*",
            ),
        )

        self.crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        logs_bucket.grant_read(
            self.crawler_role,
            objects_key_pattern=f"{logs_s3_prefix}*",
        )
        self.logs_event_queue.grant_consume_messages(self.crawler_role)

        self.crawler = glue.CfnCrawler(
            self,
            "GranuleProcessingEventsRawLogsCrawler",
            name="granule-processing-events-raw",
            role=self.crawler_role.role_arn,
            database_name=database_name,
            table_prefix="granule-processing-events-raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=logs_bucket.s3_url_for_object(key=logs_s3_prefix),
                        event_queue_arn=self.logs_event_queue.queue_arn,
                    ),
                ],
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 0 * * ? *)",
            ),
        )
