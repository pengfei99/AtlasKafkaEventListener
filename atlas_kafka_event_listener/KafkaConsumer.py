from typing import List

from atlas_client.client import Atlas
from kafka import KafkaConsumer

from atlas_kafka_event_listener.LogManager import LogManager
from atlas_kafka_event_listener.HiveEventHandler import HiveEventHandler

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init kafka consumer")


class KafkaConsumer:
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
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key.decode('utf-8'),
                                                 message.value.decode('utf-8')))

    def event_handler(self, event_key: str, event_value: str):
        if event_key == "create_table":
            self.hive_event_handler.handle_create_table_event(event_value)
        else:
            my_logger.error("Unknown event key")


def main():
    topic = "hive_meta"
    group_id = "my-group"
    broker_url = ['localhost:9092']
    start(topic, group_id, broker_url)


if __name__ == "__main__":
    main()
