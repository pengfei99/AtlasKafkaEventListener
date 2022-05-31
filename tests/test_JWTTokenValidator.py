import calendar
import time

import pytest

from atlas_kafka_event_listener.oidc.JwtTokenValidator import JwtTokenValidator
from creds import ssp_token


@pytest.fixture
def token_validator():
    return JwtTokenValidator()


def test_get_pub_key(token_validator):
    path = "/home/pliu/git/PyKeyCloakJWT/keys/id_rsa.pub"
    pub_key = token_validator.get_pub_key(path)
    print(f"pub_key: {pub_key}")
    assert pub_key is not None


def test_get_token_payload(token_validator):
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0" \
            "MjQyIiwibmFtZSI6IlBlbmdmZWkgTGl1Iiwibmlja25hbWUiOiJmY" \
            "XRtYW4ifQ.vqfXSdPTZU_o8FCxy9NZfa5Iyo47qbAdJuFvcJeR-0o"
    secret = "my_untold_secret"
    actual_value = token_validator.get_token_payload(token, secret)
    expect_value = {
        'sub': '4242',
        'name': 'Pengfei Liu',
        'nickname': 'fatman'
    }
    assert actual_value == expect_value


def test_get_token_payload_without_verification(token_validator):
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0" \
            "MjQyIiwibmFtZSI6IlBlbmdmZWkgTGl1Iiwibmlja25hbWUiOiJmY" \
            "XRtYW4ifQ.vqfXSdPTZU_o8FCxy9NZfa5Iyo47qbAdJuFvcJeR-0o"
    actual_value = token_validator.get_token_payload_without_verification(token)
    expect_value = {
        'sub': '4242',
        'name': 'Pengfei Liu',
        'nickname': 'fatman'
    }
    assert actual_value == expect_value


def test_show_token_exp_time(token_validator):
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MjQyIiwibmFtZSI6IlBlbmdmZWkg" \
            "TGl1Iiwibmlja25hbWUiOiJmYXRtYW4iLCJleHAiOjE2NTI1MjM1MzF9.IC0PXNMH4pGre1m9dCO3p70odyMtVKu3UcYsYLi0Qx4"
    token_validator.show_token_exp_time(token)


def test_is_expired(token_validator):
    actual_val = token_validator.is_expired(ssp_token)
    assert actual_val == False


def test_get_current_unix_ts(token_validator):
    expected_ts = calendar.timegm(time.gmtime())
    actual_ts = token_validator.get_current_unix_ts()
    print(f"expected value: {expected_ts}")
    print(f"actual value: {actual_ts}")
    assert actual_ts == expected_ts
