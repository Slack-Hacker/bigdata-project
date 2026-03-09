#!/bin/bash

echo "Waiting for HDFS to exit Safe Mode..."

while true
do
    status=$(hdfs dfsadmin -safemode get)
    if [[ $status == *"OFF"* ]]; then
        break
    fi
    sleep 3
done

spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--executor-cores 1 \
--executor-memory 512m \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
spark/kafka_to_hbase_stream.py &
