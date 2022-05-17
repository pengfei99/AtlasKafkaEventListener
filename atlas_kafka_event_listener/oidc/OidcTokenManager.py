import logging
import sys

from src.JwtTokenValidator import JwtTokenValidator
from src.OidcClient import OidcClient

my_logger = logging.getLogger(__name__)


class OidcTokenManager:
    def __init__(self, keycloak_server_url: str, realm_name: str, client_id: str, client_secret: str,
                 enable_service_account: bool = True, **kwargs):
        self.__url = keycloak_server_url
        self.__realm_name = realm_name
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__enable_sa = enable_service_account
        if not self.__enable_sa:
            try:
                self.__user_name = kwargs["user_name"]
                self.__user_secret = kwargs["user_secret"]
            except KeyError:
                my_logger.error("user name and secret must be provided, if you choose to use a user account")
                sys.exit(1)
        self.__oidc_client = OidcClient(keycloak_server_url=keycloak_server_url, realm_name=realm_name,
                                        client_id=client_id, client_secret=client_secret)
        self.oidc_token = self.generate_oidc_token()
        self.__jwt_token_validator = JwtTokenValidator()

    def generate_oidc_token(self) -> dict:
        """
        This method connect to a keycloak server, and generate an oidc token
        :return:
        """
        if self.__enable_sa:
            return self.__oidc_client.get_service_account_token()
        else:
            return self.__oidc_client.get_user_token(self.__user_name, self.__user_secret)

    def get_current_access_token(self) -> str:
        """
        This method returns the access token of current oidc token
        :return: str
        """
        return self.oidc_token["access_token"]

    def renew_oidc_token(self) -> bool:
        """
        This method will check if the current token is expired or not. If expired, then generate a new token and return
        True. Otherwise, do nothing and return false
        :return: bool
        """
        # if current token is expired, ask a new one from the keycloak server
        if self.__jwt_token_validator.is_expired(self.get_current_access_token()):
            my_logger.info("Current token is expired, generate a new token")
            self.oidc_token = self.generate_oidc_token()
            return True
        else:
            my_logger.info("Current token is still valid.")
            return False
