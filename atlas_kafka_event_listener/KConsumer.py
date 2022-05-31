from typing import List

from kafka import KafkaConsumer

from atlas_kafka_event_listener.HiveEventHandler import HiveEventHandler
from atlas_kafka_event_listener.LogManager import LogManager

my_logger = LogManager(__name__).get_logger()
my_logger.debug("Init kafka consumer")


class KConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        broker_url: List[str],
        atlas_hostname: str,
        atlas_port: int,
        oidc_token_manager,
    ):
        self.__topic = topic
        self.__group_id = group_id
        self.__broker_url = broker_url
        self.__hive_event_handler = HiveEventHandler(
            atlas_hostname, atlas_port, oidc_token_manager
        )

    def start(self):
        """
        This method will pull message from a kafka topic in a given kafka cluster. It then calls event_handler to treat
        each received message.
        :return:
        """
        consumer = KafkaConsumer(
            self.__topic,
            group_id=self.__group_id,
            bootstrap_servers=self.__broker_url,
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )
        for message in consumer:
            # message value and key are raw bytes we need to deserialize it
            # in our case, we use decode('utf-8') to convert byte to string by using encoding 'utf-8'
            event_key = message.key.decode("utf-8")
            event_value = message.value.decode("utf-8")
            print(
                "%s:%d:%d: key=%s value=%s"
                % (
                    message.topic,
                    message.partition,
                    message.offset,
                    event_key,
                    event_value,
                )
            )
            self.__event_handler(event_key, event_value)

    def __event_handler(self, event_key: str, event_value: str):
        """
        Handles kafka message based on message key.

        :param event_key: The kafka message key
        :param event_value: The kafka message content
        :return:
        """
        if event_key == "create_table":
            self.__hive_event_handler.handle_create_table_event(event_value)
        elif event_key == "drop_table":
            self.__hive_event_handler.handle_drop_table_event(event_value)
        elif event_key == "alter_table":
            # todo
            pass
        else:
            my_logger.error("Unknown event key")
            raise
