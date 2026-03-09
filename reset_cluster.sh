#!/bin/bash

echo "Stopping services..."

stop-hbase.sh
stop-yarn.sh
stop-dfs.sh

pkill -f kafka
pkill -f zookeeper
pkill -f SparkSubmit

echo "Cleaning temp files..."

rm -rf /tmp/zookeeper
rm -rf ~/kafka-zk/kafka-logs/*
rm -rf ~/spark-checkpoints

mkdir -p ~/kafka-zk/kafka-logs

echo "Reset complete."
