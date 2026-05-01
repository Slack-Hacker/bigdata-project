#!/bin/bash
# Creates the Kafka topic used by the pipeline (idempotent)

~/kafka-zk/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic website_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Topic 'website_logs' is ready."
