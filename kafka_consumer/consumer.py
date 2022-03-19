from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer('hive-meta',
                             group_id='my-group',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=False)
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key.decode('utf-8'),
                                             message.value.decode('utf-8')))


if __name__ == "__main__":
    main()