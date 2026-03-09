# Website Traffic Monitoring System

Real-time website analytics using:

Kafka + Spark Streaming + HBase + Node.js

## Start Cluster

start_cluster.sh

## Start Spark Streaming

start_streaming.sh

## Start Website

cd website
node server.js

## View Data

hbase shell
scan 'web_traffic'
