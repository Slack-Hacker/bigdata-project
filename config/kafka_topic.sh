#!/bin/bash

~/kafka-zk/bin/kafka-topics.sh \
--create \
--topic website_logs \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
