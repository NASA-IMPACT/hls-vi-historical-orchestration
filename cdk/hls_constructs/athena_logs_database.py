import base64
import datetime as dt
import json
from typing import Any

from aws_cdk import (
    Aws,
    RemovalPolicy,
    aws_glue as glue,
    aws_s3 as s3,
)
from constructs import Construct

# Obtained from running AWS Glue crawler to infer the schema
ATHENA_STRUCT_AWS_BATCH_JOB_INFO = "struct<jobArn:string,jobName:string,jobId:string,jobQueue:string,status:string,attempts:array<struct<container:struct<containerInstanceArn:string,taskArn:string,exitCode:int,logStreamName:string,networkInterfaces:array<string>>,startedAt:bigint,stoppedAt:bigint,statusReason:string>>,statusReason:string,createdAt:bigint,retryStrategy:struct<attempts:int,evaluateOnExit:array<struct<onReason:string,action:string,onStatusReason:string>>>,startedAt:bigint,stoppedAt:bigint,dependsOn:array<string>,jobDefinition:string,parameters:string,container:struct<image:string,command:array<string>,jobRoleArn:string,executionRoleArn:string,volumes:array<string>,environment:array<struct<name:string,value:string>>,mountPoints:array<string>,readonlyRootFilesystem:boolean,ulimits:array<string>,exitCode:int,containerInstanceArn:string,taskArn:string,logStreamName:string,networkInterfaces:array<string>,resourceRequirements:array<struct<value:string,type:string>>,logConfiguration:struct<logDriver:string,options:struct<awslogs-group:string,awslogs-region:string,awslogs-stream-prefix:string>,secretOptions:array<string>>,secrets:array<string>>,timeout:struct<attemptDurationSeconds:int>,tags:struct<resourceArn:string>,propagateTags:boolean,platformCapabilities:array<string>,eksAttempts:array<string>,isTerminated:boolean>"

COMMON_TABLE_COLUMNS = {
    "outcome": glue.CfnTable.ColumnProperty(
        name="outcome",
        comment="The processing event outcome (failed, success).",
        type="String",
    ),
    "platform": glue.CfnTable.ColumnProperty(
        name="platform",
        comment="The platform (L30, S30).",
        type="String",
    ),
    "acquisition_date": glue.CfnTable.ColumnProperty(
        name="acquisition_date",
        comment="The acquisition date.",
        type="timestamp",
    ),
    "granule_id": glue.CfnTable.ColumnProperty(
        name="granule_id",
        comment="HLS granule ID.",
        type="String",
    ),
    "attempt": glue.CfnTable.ColumnProperty(
        name="attempt",
        comment="Attempt.",
        type="int",
    ),
    "last_modified_date": glue.CfnTable.ColumnProperty(
        name="last_modified_date",
        comment="The event log's creation date or the last modified date, whichever is the latest.",
        type="timestamp",
    ),
    "key": glue.CfnTable.ColumnProperty(
        name="key",
        comment="The event log S3 key.",
        type="String",
    ),
}


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

        self.granule_processing_events_raw_table = (
            self._create_granule_processing_events_raw_logs_table(
                database_name=database_name,
                logs_bucket=logs_bucket,
                logs_s3_prefix=logs_s3_prefix,
            )
        )

        self.granule_processing_events_raw_failures_view = (
            self._create_granule_processing_events_raw_failures_view(
                database_name=database_name,
                raw_table=self.granule_processing_events_raw_table,
            )
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
                        type="string",
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                            name="bucket",
                            comment="The name of the bucket that the inventory is for.",
                            type="string",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="key",
                            comment="The object key name (or key) that uniquely identifies the object in the bucket.",
                            type="string",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="version_id",
                            comment="The object version ID.",
                            type="string",
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
        FROM "{logs_s3_inventory_table_name}"
        WHERE
            dt = (SELECT max(dt) FROM "{logs_s3_inventory_table_name}")
            AND is_latest
            AND NOT is_delete_marker
        """

        columns = [
            COMMON_TABLE_COLUMNS[column_name]
            for column_name in (
                "outcome",
                "platform",
                "acquisition_date",
                "granule_id",
                "attempt",
                "last_modified_date",
                "key",
            )
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
                        type="string",
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

    def _create_granule_processing_events_raw_logs_table(
        self,
        *,
        database_name: str,
        logs_bucket: s3.IBucket,
        logs_s3_prefix: str,
    ) -> glue.CfnTable:
        """Create raw logs Glue table for JSON formatted job logs

        This table uses partition projection on (outcome, platform, acquisition_date)
        and assumes that:
          - "outcome" is an enum (success | failure)
          - "platform" is an enum (S30 | L30)
          - "acquisition_date" is from 2013-01-01 to NOW

        """
        # Do not partition on granule_id because we can't know it ahead of time and
        # don't want to "inject" it since we'd require it for every query.
        columns = [
            COMMON_TABLE_COLUMNS["granule_id"],
            COMMON_TABLE_COLUMNS["attempt"],
            glue.CfnTable.ColumnProperty(
                name="job_info",
                type=ATHENA_STRUCT_AWS_BATCH_JOB_INFO,
            ),
        ]
        partition_keys = [
            glue.CfnTable.ColumnProperty(name="outcome", type="string"),
            glue.CfnTable.ColumnProperty(name="platform", type="string"),
            glue.CfnTable.ColumnProperty(name="acquisition_date", type="string"),
        ]

        s3_location = logs_bucket.s3_url_for_object(logs_s3_prefix)
        table_properties = {
            "classification": "json",
            "projection.enabled": "true",
            "projection.outcome.type": "enum",
            "projection.outcome.values": "success,failure",
            "projection.platform.type": "enum",
            "projection.platform.values": "S30,L30",
            "projection.acquisition_date.type": "date",
            "projection.acquisition_date.range": "2013-01-01,NOW",
            "projection.acquisition_date.format": "yyyy-MM-dd",
            "projection.acquisition_date.interval": "1",
            "projection.acquisition_date.interval.unit": "DAYS",
            "storage.location.template": f"{s3_location}/outcome=${{outcome}}/platform=${{platform}}/acquisition_date=${{acquisition_date}}/",
        }

        serde_info = glue.CfnTable.SerdeInfoProperty(
            serialization_library="org.openx.data.jsonserde.JsonSerDe",
            parameters={"case.insensitive": "true", "dots.in.keys": "false"},
        )

        storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
            columns=columns,
            location=s3_location,
            input_format="org.apache.hadoop.mapred.TextInputFormat",
            output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_info=serde_info,
            compressed=False,
        )

        return glue.CfnTable(
            self,
            "GranuleProcessingEventsRawLogs",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="granule-processing-events-raw",
                table_type="EXTERNAL_TABLE",
                description="Raw logs database for HLS-VI Historical Orchestration tasks.",
                parameters=table_properties,
                partition_keys=partition_keys,
                storage_descriptor=storage_descriptor,
            ),
        )

    def _create_granule_processing_events_raw_failures_view(
        self,
        *,
        database_name: str,
        raw_table: glue.CfnTable,
    ) -> glue.CfnTable:
        """Create enriched view of JSON formatted job logs for failures"""
        assert isinstance(raw_table.table_input, glue.CfnTable.TableInputProperty)
        sql = rf"""
        SELECT
            MAX_BY(outcome, attempt) AS outcome,
            ARBITRARY(platform) AS platform,
            ARBITRARY(date_parse(acquisition_date, '%Y-%m-%d')) AS acquisition_date,
            granule_id AS granule_id,
            MAX_BY(attempt, attempt) AS attempt,
            ELEMENT_AT(MAX_BY(job_info.attempts, attempt), -1).container.exitcode AS exit_code,
            from_unixtime(ELEMENT_AT(MAX_BY(job_info.attempts, attempt), -1).startedat / 1000) AS started_at,
            from_unixtime(ELEMENT_AT(MAX_BY(job_info.attempts, attempt), -1).stoppedat / 1000) AS stopped_at,
            ELEMENT_AT(MAX_BY(job_info.attempts, attempt), -1).statusreason AS status_reason,
            ELEMENT_AT(MAX_BY(job_info.attempts, attempt), -1).container.logstreamname AS logstream_name,
            MAX_BY("$path", attempt) AS key
        FROM "{raw_table.table_input.name}"
        WHERE outcome = 'failure'
        GROUP BY granule_id
        """

        columns = [
            COMMON_TABLE_COLUMNS["outcome"],
            COMMON_TABLE_COLUMNS["platform"],
            COMMON_TABLE_COLUMNS["acquisition_date"],
            COMMON_TABLE_COLUMNS["granule_id"],
            COMMON_TABLE_COLUMNS["attempt"],
            glue.CfnTable.ColumnProperty(
                name="exit_code",
                comment="AWS Batch job exit code.",
                type="int",
            ),
            glue.CfnTable.ColumnProperty(
                name="started_at",
                comment="AWS Batch job stop datetime.",
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="stopped_at",
                comment="AWS Batch job start datetime.",
                type="timestamp",
            ),
            glue.CfnTable.ColumnProperty(
                name="status_reason",
                comment="AWS Batch job exit status reason.",
                type="String",
            ),
            glue.CfnTable.ColumnProperty(
                name="logstream_name",
                comment="AWS Batch job logstream name.",
                type="String",
            ),
            COMMON_TABLE_COLUMNS["key"],
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
            "GranuleProcessingEventsFailuresView",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="granule-processing-events-failures",
                table_type="VIRTUAL_VIEW",
                parameters={"presto_view": "true", "comment": "Presto View"},
                partition_keys=raw_table.table_input.partition_keys,
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                ),
                view_original_text=f"/* Presto View: {sql_b64} */",
                view_expanded_text="/* Presto View */",
            ),
        )
        view.apply_removal_policy(RemovalPolicy.DESTROY)
        view.add_dependency(raw_table)
        return view
