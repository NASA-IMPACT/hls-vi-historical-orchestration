"""Earthdata Login Credential Rotator

This Lambda function is responsible for storing fresh S3 credentials in
SecretsManager for other pipeline actors to access.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
import requests

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import CloudWatchEventsMessageEvent


LPDAAC_S3_CREDENTIALS_URL = "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"


class SessionWithHeaderRedirection(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username: str, password: str) -> None:
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(
        self, prepared_request: requests.PreparedRequest, response: requests.Response
    ) -> None:
        headers = prepared_request.headers
        url = prepared_request.url

        if "Authorization" in headers:
            original_parsed = urlparse(response.request.url)
            redirect_parsed = urlparse(url)

            if (
                (original_parsed.hostname != redirect_parsed.hostname)
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]

        return


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

    session = SessionWithHeaderRedirection(username, password)

    response = session.get(LPDAAC_S3_CREDENTIALS_URL)
    response.raise_for_status()

    s3_credentials = response.json()

    secrets.update_secret(
        SecretId=s3_credentials_secret_id,
        SecretString=json.dumps(
            {
                "SECRET_ACCESS_KEY": s3_credentials["secretAccessKey"],
                "ACCESS_KEY_ID": s3_credentials["accessKeyId"],
                "SESSION_TOKEN": s3_credentials["sessionToken"],
            }
        ),
    )


def handler(event: CloudWatchEventsMessageEvent, context: Context) -> None:
    user_pass_secret_id = os.environ["USER_PASS_SECRET_ID"]
    s3_credentials_secret_id = os.environ["S3_CREDENTIALS_SECRET_ID"]

    edl_credential_rotator(user_pass_secret_id, s3_credentials_secret_id)
