#!/bin/bash

python ../atlas_kafka_event_listener/main.py "$ATLAS_HOSTNAME" "$ATLAS_PORT" "$OIDC_TOKEN" "$KAFKA_BROKER_URL" "$KAFKA_TOPIC_NAME" "$CONSUMER_GROUP_ID"
