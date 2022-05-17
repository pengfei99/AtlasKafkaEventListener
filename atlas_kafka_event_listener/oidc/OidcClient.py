import json
import logging

import requests
from keycloak import KeycloakOpenID
from requests.auth import HTTPBasicAuth

my_logger = logging.getLogger(__name__)


class OidcClient:
    def __init__(self, keycloak_server_url, realm_name, client_id, client_secret):
        self.__url = keycloak_server_url
        self.__realm_name = realm_name
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__oidc_client = KeycloakOpenID(server_url=keycloak_server_url, realm_name=realm_name, client_id=client_id,
                                            client_secret_key=client_secret)

    def get_user_token(self, user_name, user_secret):
        return self.__oidc_client.token(user_name, user_secret)

    def get_service_account_token(self):
        payload = "grant_type=client_credentials"
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        endpoint = f"{self.__url}/realms/{self.__realm_name}/protocol/openid-connect/token"
        response = requests.request("POST", endpoint, headers=headers, data=payload,
                                    auth=HTTPBasicAuth(self.__client_id, self.__client_secret))
        if response.status_code == 200:
            oidc_token_str = response.text
            oidc_token = json.loads(oidc_token_str)
            return oidc_token
        else:
            my_logger.exception(f"Request failed with status code {response.status_code}."
                                f"\n The error message is {response.text}")
            return None
