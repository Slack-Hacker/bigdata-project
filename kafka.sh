#!/bin/bash

echo "====================================="
echo "🚀 STARTING KAFKA"
echo "====================================="

echo ""
echo "1️⃣ Stopping any existing Kafka process..."

pkill -f kafka.Kafka
sleep 3

echo ""
echo "2️⃣ Cleaning old Kafka logs..."

rm -rf ~/kafka-zk/kafka-logs

sleep 2

echo ""
echo "3️⃣ Starting Kafka..."

~/kafka-zk/bin/kafka-server-start.sh -daemon ~/kafka-zk/config/server.properties

sleep 8

echo ""
echo "4️⃣ Checking Kafka status..."

if jps | grep -q Kafka; then
    echo "✅ Kafka started successfully"
else
    echo "❌ Kafka failed to start"
fi

echo ""
echo "====================================="
