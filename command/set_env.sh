export ATLAS_HOSTNAME=https://atlas.lab.sspcloud.fr
export ATLAS_PORT=443
export KAFKA_BROKER_URL=kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092,kafka-2.kafka-headless:9092
export KAFKA_TOPIC_NAME=hive-meta
export CONSUMER_GROUP_ID=hive_atlas_meta
export KC_URL=https://auth.lab.sspcloud.fr/auth
export REALM_NAME=sspcloud
export CLIENT_ID=atlas-sa
export CLIENT_SECRET=

export PYTHONPATH="${PYTHONPATH}:/home/pliu/git/AtlasKafkaEventListener"