#!/bin/bash

echo "Stopping Spark..."
yarn application -kill $(yarn application -list | grep Spark | awk '{print $1}')

echo "Stopping HBase..."
stop-hbase.sh

echo "Stopping Kafka..."
pkill -f kafka

echo "Stopping ZooKeeper..."
pkill -f zookeeper

echo "Stopping YARN..."
stop-yarn.sh

echo "Stopping HDFS..."
stop-dfs.sh

echo "clearing website server"
pkill -f server.js


echo "Cluster stopped."
