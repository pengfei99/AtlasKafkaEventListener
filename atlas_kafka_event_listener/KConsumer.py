from typing import List

from kafka import KafkaConsumer

from atlas_kafka_event_listener.HiveEventHandler import HiveEventHandler
from atlas_kafka_event_listener.LogManager import LogManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init kafka consumer")


class KConsumer:
    def __init__(self, topic: str, group_id: str, broker_url: List[str], atlas_hostname: str, atlas_port: int,
                 oidc_token_manager):
        self.topic = topic
        self.group_id = group_id
        self.broker_url = broker_url
        self.hive_event_handler = HiveEventHandler(atlas_hostname, atlas_port, oidc_token_manager)

    def start(self):
        consumer = KafkaConsumer(self.topic,
                                 group_id=self.group_id,
                                 bootstrap_servers=self.broker_url,
                                 auto_offset_reset='latest',
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
        elif event_key == "drop_table":
            self.hive_event_handler.handle_drop_table_event(event_value)
        elif event_key == "alter_table":
            # todo
            pass
        else:
            my_logger.error("Unknown event key")
            raise
