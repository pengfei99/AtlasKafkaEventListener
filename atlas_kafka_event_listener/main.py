import sys

from atlas_kafka_event_listener.KConsumer import KConsumer
from atlas_kafka_event_listener.LogManager import LogManager
from atlas_kafka_event_listener.oidc.OidcTokenManager import OidcTokenManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init atlas kafka event listener")


def main():
    if len(sys.argv) != 10:
        my_logger.error(
            "Number of arguments that you give is wrong, please enter the following parameter in order."
            "<atlas_hostname> <atlas_port> <kafka_broker_urls> <kafka_topic> <consumer_group_id>"
            "<keycloak_server_url> <realm_name> <client_id> <client_secret>"
        )
        exit(1)
    else:
        atlas_hostname = sys.argv[1]
        atlas_port = sys.argv[2]

        kafka_broker_urls = sys.argv[3]
        kafka_topic = sys.argv[4]
        consumer_group_id = sys.argv[5]

        keycloak_url = sys.argv[6]
        realm_name = sys.argv[7]
        client_id = sys.argv[8]
        client_secret = sys.argv[9]

    # create an instance of the OidcTokenManager
    oidc_token_manager = OidcTokenManager(
        keycloak_url, realm_name, client_id, client_secret
    )

    broker_url_list = kafka_broker_urls.replace(" ", "").split(",")
    consumer = KConsumer(
        kafka_topic,
        consumer_group_id,
        broker_url_list,
        atlas_hostname=atlas_hostname,
        atlas_port=int(atlas_port),
        oidc_token_manager=oidc_token_manager,
    )
    consumer.start()


if __name__ == "__main__":
    main()
