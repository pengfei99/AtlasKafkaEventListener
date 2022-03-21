from typing import List

from atlas_client.client import Atlas
from kafka import KafkaConsumer

from atlas_kafka_event_listener import secret
from atlas_kafka_event_listener.HiveEventHandler import HiveEventHandler
from atlas_kafka_event_listener.LogManager import LogManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init kafka consumer")


class KConsumer:
    def __init__(self, topic: str, group_id: str, broker_url: List[str], atlas_client: Atlas):
        self.topic = topic
        self.group_id = group_id
        self.broker_url = broker_url
        self.hive_event_handler = HiveEventHandler(atlas_client)

    def start(self):
        consumer = KafkaConsumer(self.topic,
                                 group_id=self.group_id,
                                 bootstrap_servers=self.broker_url,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=False)
        for message in consumer:
            # message value and key are raw bytes we need to deserialize it
            # in our case, we use decode('utf-8') to convert byte to string by using encoding 'utf-8'
            event_key = message.key.decode('utf-8')
            event_value = message.value.decode('utf-8')
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, event_key, event_value))
            self.event_handler(event_key, event_value)

    def event_handler(self, event_key: str, event_value: str):
        if event_key == "create_table":
            self.hive_event_handler.handle_create_table_event(event_value)
        else:
            my_logger.error("Unknown event key")


def main():
    # create an atlas consumer instance
    local = False
    # config for atlas client
    atlas_prod_hostname = "https://atlas.lab.sspcloud.fr"
    atlas_prod_port = 443
    atlas_local_hostname = "http://localhost"
    login = "admin"
    pwd = "admin"

    if local:
        atlas_client = Atlas(atlas_local_hostname, port=21000, username=login, password=pwd)
    else:
        # create an instance of the atlas Client with oidc token
        atlas_client = Atlas(atlas_prod_hostname, atlas_prod_port, oidc_token=secret.oidc_token)
    topic = "hive_meta"
    group_id = "my-group"
    broker_url = ['localhost:9092']

    consumer = KConsumer(topic, group_id, broker_url, atlas_client=atlas_client)
    consumer.start()


if __name__ == "__main__":
    main()
