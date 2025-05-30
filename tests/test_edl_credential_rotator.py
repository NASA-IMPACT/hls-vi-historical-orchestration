"""Tests for `edl_credential_rotator`"""

import responses

from edl_credential_rotator.handler import (
    EDL_AUTH_HOST,
    LPDAAC_S3_CREDENTIALS_URL,
    S3Credentials,
    fetch_s3_credentials,
)

EXPECTED_REDIRECT = f"https://{EDL_AUTH_HOST}/oauth/authorize"


@responses.activate
def test_fetch_s3_credentials_correct_redirect() -> None:
    """Test credential fetching works with the expected redirect"""
    resp1 = responses.Response(
        responses.GET,
        LPDAAC_S3_CREDENTIALS_URL,
        status=307,
        headers={"location": EXPECTED_REDIRECT},
    )
    resp2 = responses.Response(
        responses.GET,
        EXPECTED_REDIRECT,
        json={
            "secretAccessKey": "foo",
            "accessKeyId": "baz",
            "sessionToken": "buz",
        },
    )

    responses.add(resp1)
    responses.add(resp2)

    s3creds = fetch_s3_credentials("user", "password")
    # TypedDict doesn't support isinstance checks, but we can check the
    # keys match the expected type annotation
    assert list(s3creds) == list(S3Credentials.__annotations__)
    assert resp1.call_count == 1
    assert resp2.call_count == 1
    assert "Authorization" in resp2.calls[0].request.headers


@responses.activate
def test_fetch_s3_credentials_no_auth_bad_redirect() -> None:
    """Test credential fetching doesn't send user/password to unknown redirect"""
    redirect = "https://test.bad/"
    resp1 = responses.Response(
        responses.GET,
        LPDAAC_S3_CREDENTIALS_URL,
        status=307,
        headers={"location": redirect},
    )
    resp2 = responses.Response(
        responses.GET,
        redirect,
        json={
            "secretAccessKey": "foo",
            "accessKeyId": "baz",
            "sessionToken": "buz",
        },
    )

    responses.add(resp1)
    responses.add(resp2)

    fetch_s3_credentials("user", "password")
    assert resp1.call_count == 1
    assert resp2.call_count == 1
    # If our redirect was bad, ensure we don't pass our user/password via auth header
    assert "Authorization" not in resp2.calls[0].request.headers
