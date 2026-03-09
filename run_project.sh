#!/bin/bash

echo "====================================="
echo "🚀 STARTING BIG DATA PROJECT"
echo "====================================="

echo ""
echo "1️⃣ Starting Hadoop (HDFS + YARN)..."
start-dfs.sh
start-yarn.sh

sleep 5

echo ""
echo "2️⃣ Starting ZooKeeper..."
~/kafka-zk/bin/zookeeper-server-start.sh -daemon ~/kafka-zk/config/zookeeper.properties

sleep 10

echo ""
echo "3️⃣ Starting Kafka..."

echo "Cleaning old Kafka logs..."
rm -rf ~/kafka-zk/kafka-logs

~/kafka-zk/bin/kafka-server-start.sh -daemon ~/kafka-zk/config/server.properties

sleep 8

echo "Kafka Status:"
jps | grep Kafka

echo ""
echo "4️⃣ Starting HBase..."
start-hbase.sh

sleep 5

echo ""
echo "5️⃣ Starting HBase Thrift..."
hbase-daemon.sh start thrift

sleep 5

echo ""
echo "6️⃣ Starting Spark Streaming..."

cd ~/bigdata-project
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


sleep 5

echo ""
echo "7️⃣ Starting Dashboard Server..."

cd ~/bigdata-project/dashboard

python3 dashboard_server.py > dashboard.log 2>&1 &

sleep 3

echo ""
echo "8️⃣ Starting Website Server..."

cd ~/website-traffic-monitoring-system

node server.js > website.log 2>&1 &

echo ""
echo "====================================="
echo "✅ PROJECT STARTED SUCCESSFULLY"
echo "====================================="

echo ""
echo "🌐 Website:"
echo "http://localhost:3001"

echo ""
echo "📊 Dashboard:"
echo "http://localhost:5000/stats"

echo ""
echo "📡 Spark UI:"
echo "http://master:4040"

echo ""
echo "🎯 HBase Table:"
echo "hbase shell → scan 'web_traffic'"
