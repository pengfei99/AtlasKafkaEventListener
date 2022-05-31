import pytest

from atlas_kafka_event_listener.oidc.OidcClient import OidcClient


@pytest.fixture
def oidc_client():
    #  for keycloak version< 17.0 the url should be
    legacy_keycloak_url = "http://localhost:8080/auth"
    #  for keycloak version >=17.0, the url should be
    keycloak_url = "http://localhost:8080"
    realm_name = "pengfei-test"
    client_id = "pengfei-dv-app"
    client_secret = "8BJOcHtXlXpKVLGFOjGoAJ4nxo1WyNKT"
    return OidcClient(keycloak_server_url=keycloak_url, realm_name=realm_name,
                      client_id=client_id, client_secret=client_secret)


def test_get_user_token(oidc_client):
    user_name = "pengfei"
    user_secret = "toto"
    oidc_token = oidc_client.get_user_token(user_name, user_secret)
    print(f"oidc token: {oidc_token}")
    access_token = oidc_token["access_token"]
    print(f"access token: {access_token}")
    assert access_token is not None


def test_service_account(oidc_client):
    oidc_token = oidc_client.get_service_account_token()
    print(f"oidc token: {oidc_token}")
    access_token = oidc_token["access_token"]
    print(f"access token: {access_token}")
    assert access_token is not None
