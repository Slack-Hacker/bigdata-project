#!/bin/bash
<<<<<<< HEAD
# Creates the Kafka topic used by the pipeline (idempotent)

~/kafka-zk/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic website_logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Topic 'website_logs' is ready."
=======

~/kafka-zk/bin/kafka-topics.sh \
--create \
--topic website_logs \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
>>>>>>> 3e8e9d96940598f46741697fb7af1a50fd08e4b3
