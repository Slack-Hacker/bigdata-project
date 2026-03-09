#!/bin/bash

echo "Starting HDFS..."
start-dfs.sh
sleep 5

echo "Starting YARN..."
start-yarn.sh
sleep 5

echo "Starting ZooKeeper..."
~/kafka-zk/bin/zookeeper-server-start.sh -daemon ~/kafka-zk/config/zookeeper.properties
sleep 5

echo "Starting Kafka..."
~/kafka-zk/bin/kafka-server-start.sh -daemon ~/kafka-zk/config/server.properties
sleep 5

echo "Starting HBase..."
start-hbase.sh
sleep 10

echo "Starting Thrift..."
hbase-daemon.sh start thrift
sleep 5

echo "Cluster started successfully!"
