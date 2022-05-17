import logging
import os
from datetime import datetime
from typing import Union

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.dsa import DSAPublicKey
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from jwt import ExpiredSignatureError

my_logger = logging.getLogger(__name__)


class JwtTokenValidator:
    DEFAULT_MARGIN = 86400

    def __init__(self, pub_key_path: str = None):
        self.__pub_key = self.get_pub_key(pub_key_path)

    @staticmethod
    def get_pub_key(pub_key_path: str) -> Union[
        EllipticCurvePublicKey, RSAPublicKey, DSAPublicKey, Ed25519PublicKey, None]:
        """
        This method takes a local path of a public key file and return it in serialized public key object
        :param pub_key_path: local path of the public key file
        :return:
        """
        if pub_key_path:
            public_key = open(pub_key_path, 'r').read()
            return serialization.load_ssh_public_key(public_key.encode())
        else:
            return None

    def get_token_payload(self, input_token: str, custom_secret: str = None) -> dict:
        """
        Standard way to get the jwt token payload, With verification of signature and expiration time
        :param input_token: the input token that we want to analyze
        :return:
        """
        if custom_secret:
            secret = custom_secret
        else:
            secret = self.__pub_key
        payload = None
        header = jwt.get_unverified_header(input_token)
        try:
            payload = jwt.decode(
                input_token,
                key=secret,
                algorithms=[header['alg'], ],
                options={"verify_exp": False
                         }
            )
            my_logger.debug(f"Read token payload: {payload}")
        except ExpiredSignatureError as error:
            my_logger.error(f'Unable to decode the token, error: {error}')
        return payload

    @staticmethod
    def get_token_payload_without_verification(input_token: str) -> dict:
        """
        Get the token payload by ignoring the signature and expiration date
        :param input_token: the input token that we want to analyze
        :return:
        """
        payload = jwt.decode(input_token,
                             options={"verify_signature": False,
                                      "verify_exp": False
                                      }
                             )
        my_logger.debug(f"Read token payload: {payload}")
        return payload

    @staticmethod
    def show_token_exp_time(input_token: str):
        """
        This method shows the expiration time of the given token
        :param input_token: the input token that we want to analyze
        :return:
        """
        payload = jwt.decode(input_token,
                             options={"verify_signature": False,
                                      "verify_exp": False
                                      }
                             )
        # token_expiration has int type
        token_expiration = payload["exp"]
        utc_time = datetime.utcfromtimestamp(token_expiration).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Token expiration time in unix ts: {token_expiration}")
        print(f"Token expiration time in UTC format: {utc_time}")

    @staticmethod
    def get_current_unix_ts():
        """
        This method returns current time in unix timestamp
        :return:
        """
        return int(datetime.now().timestamp())

    def is_expired(self, token: str):
        """
        This method checks if a given token is expired or not
        :param token: the input token that we want to analyze
        :return:
        """
        # get expiration margin
        margin = os.getenv("TOKEN_EXP_MARGIN", JwtTokenValidator.DEFAULT_MARGIN)
        # get current timestamp
        current_ts = JwtTokenValidator.get_current_unix_ts()
        # get token payload
        if self.__pub_key:
            payload = self.get_token_payload(token)
        else:
            payload = self.get_token_payload_without_verification(token)
        token_expiration = payload["exp"]
        return True if current_ts + margin > token_expiration else False
