import pytest

from atlas_kafka_event_listener.oidc.OidcTokenManager import OidcTokenManager


@pytest.fixture
def service_account():
    keycloak_url = "http://localhost:8080"
    realm_name = "pengfei-test"
    client_id = "pengfei-dv-app"
    client_secret = "8BJOcHtXlXpKVLGFOjGoAJ4nxo1WyNKT"
    return OidcTokenManager(keycloak_server_url=keycloak_url, realm_name=realm_name, client_id=client_id,
                            client_secret=client_secret)


@pytest.fixture
def user_account():
    keycloak_url = "http://localhost:8080"
    realm_name = "pengfei-test"
    client_id = "pengfei-dv-app"
    client_secret = "8BJOcHtXlXpKVLGFOjGoAJ4nxo1WyNKT"
    user_name = "pengfei"
    user_secret = "toto"
    return OidcTokenManager(keycloak_server_url=keycloak_url, realm_name=realm_name, client_id=client_id,
                            client_secret=client_secret, enable_service_account=False, user_name=user_name,
                            user_secret=user_secret)


def test_generate_oidc_token_with_custom_account(user_account):
    token = user_account.generate_oidc_token()
    print(f"oidc token {token}")
    assert token is not None


def test_generate_oidc_token_with_service_account(service_account):
    token = service_account.generate_oidc_token()
    print(f"oidc token {token}")
    assert token is not None


def test_get_current_access_token(service_account):
    access_token = service_account.get_current_access_token()
    print(f"access token {access_token}")
    assert access_token is not None
