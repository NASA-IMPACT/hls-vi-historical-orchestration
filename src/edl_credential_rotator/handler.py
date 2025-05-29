"""Earthdata Login Credential Rotator

This Lambda function is responsible for storing fresh S3 credentials in
SecretsManager for other pipeline actors to access.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, TypedDict
from urllib.parse import urlparse

import boto3
import requests

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import CloudWatchEventsMessageEvent


LPDAAC_S3_CREDENTIALS_URL = "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
EDL_AUTH_HOST = "urs.earthdata.nasa.gov"


class S3Credentials(TypedDict):
    """AWS credentials permitting direct S3 access"""

    secret_access_key: str
    access_key_id: str
    session_token: str


def fetch_s3_credentials(username: str, password: str) -> S3Credentials:
    """Fetch S3 credentials from LPDAAC /s3credentials endpoint"""
    with requests.Session() as session:
        with session.get(LPDAAC_S3_CREDENTIALS_URL, allow_redirects=False) as r:
            r.raise_for_status()
            location = r.headers["location"]

        # We were redirected, so we must use basic auth credentials with the
        # redirect location.  If the host of the redirect is the same host we have
        # creds for, pass them along.

        redirect_host = str(urlparse(location).hostname)
        auth = (username, password) if redirect_host == EDL_AUTH_HOST else None

        with session.get(location, auth=auth) as r:
            r.raise_for_status()

            try:
                s3_credentials = r.json()
            except json.JSONDecodeError:
                # Content is not JSON; basic auth creds are invalid or not supplied
                r.status_code = 401
                r.reason = "Unauthorized"
                r.raise_for_status()

    return S3Credentials(
        secret_access_key=s3_credentials["secretAccessKey"],
        access_key_id=s3_credentials["accessKeyId"],
        session_token=s3_credentials["sessionToken"],
    )


def edl_credential_rotator(
    user_pass_secret_id: str, s3_credentials_secret_id: str
) -> None:
    """Fetch user/pass credentials, call EDL to get S3 credentials, and persist"""
    secrets = boto3.client("secretsmanager")

    username_password = json.loads(
        secrets.get_secret_value(
            SecretId=user_pass_secret_id,
        )["SecretString"]
    )
    username = username_password["USERNAME"]
    password = username_password["PASSWORD"]

    s3_credentials = fetch_s3_credentials(username, password)

    secrets.update_secret(
        SecretId=s3_credentials_secret_id,
        SecretString=json.dumps(
            {
                "SECRET_ACCESS_KEY": s3_credentials["secret_access_key"],
                "ACCESS_KEY_ID": s3_credentials["access_key_id"],
                "SESSION_TOKEN": s3_credentials["session_token"],
            }
        ),
    )


def handler(event: CloudWatchEventsMessageEvent, context: Context) -> None:
    user_pass_secret_id = os.environ["USER_PASS_SECRET_ID"]
    s3_credentials_secret_id = os.environ["S3_CREDENTIALS_SECRET_ID"]

    edl_credential_rotator(user_pass_secret_id, s3_credentials_secret_id)
