import sys

from atlas_client.client import Atlas

from atlas_kafka_event_listener.KConsumer import KConsumer
from atlas_kafka_event_listener.LogManager import LogManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init atlas kafka event listener")


def main():
    if len(sys.argv) != 7:
        my_logger.error(
            'Number of arguments that you give is wrong, please enter the following parameter in order.'
            '<atlas_hostname> <atlas_port> <oidc_token> <kafka_broker_urls> <kafka_topic> <consumer_group_id>')
        exit(1)
    else:
        atlas_hostname = sys.argv[1]
        atlas_port = sys.argv[2]
        oidc_token = sys.argv[3]
        kafka_broker_urls = sys.argv[4]
        kafka_topic = sys.argv[5]
        consumer_group_id = sys.argv[6]

    # create an instance of the atlas Client with oidc token
    atlas_client = Atlas(atlas_hostname, atlas_port, oidc_token=oidc_token)
    broker_url_list = kafka_broker_urls.replace(" ", "").split(",")
    consumer = KConsumer(kafka_topic, consumer_group_id, broker_url_list, atlas_client=atlas_client)
    consumer.start()


if __name__ == "__main__":
    main()
