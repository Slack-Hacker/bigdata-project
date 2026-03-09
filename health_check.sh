#!/bin/bash

echo "===== JVM SERVICES ====="
jps

echo ""
echo "===== YARN NODES ====="
yarn node -list

echo ""
echo "===== KAFKA TOPICS ====="
~/kafka-zk/bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092

echo ""
echo "===== HBASE TABLES ====="

echo "list" | hbase shell -n
