#!/bin/bash
# ─────────────────────────────────────────────────────────
# reset_cluster.sh — FULL RESET (cluster + data)
# Stops everything and wipes data safely
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "♻️ FULL CLUSTER RESET"
echo "====================================="

# ── 1. Stop all services ────────────────────────────────
echo ""
echo "1️⃣ Stopping all services..."

cd ~/spark 2>/dev/null || true
sbin/stop-all.sh 2>/dev/null || true

stop-hbase.sh 2>/dev/null || true
hbase-daemon.sh stop thrift 2>/dev/null || true

stop-yarn.sh 2>/dev/null || true
stop-dfs.sh 2>/dev/null || true

pkill -f kafka.Kafka        2>/dev/null || true
pkill -f kafka-server-start 2>/dev/null || true
pkill -f QuorumPeerMain     2>/dev/null || true
pkill -f SparkSubmit        2>/dev/null || true

sleep 5

# ── 2. Clean ZooKeeper ──────────────────────────────────
echo ""
echo "2️⃣ Cleaning ZooKeeper data..."

rm -rf ~/zookeeper-data
rm -rf /tmp/zookeeper

# ── 3. Clean Kafka ──────────────────────────────────────
echo ""
echo "3️⃣ Cleaning Kafka logs..."

rm -rf ~/kafka-zk/kafka-logs
mkdir -p ~/kafka-zk/kafka-logs

# ── 4. Clean Spark ──────────────────────────────────────
echo ""
echo "4️⃣ Cleaning Spark temp + checkpoints..."

rm -rf /tmp/spark-*

# Remove HDFS checkpoint dir safely
start-dfs.sh > /dev/null 2>&1
sleep 8
hdfs dfsadmin -safemode leave > /dev/null 2>&1 || true
hdfs dfs -rm -r -f /user/ankit/spark-checkpoints 2>/dev/null || true
stop-dfs.sh > /dev/null 2>&1

# ── 5. Clean application logs ───────────────────────────
echo ""
echo "5️⃣ Cleaning application logs..."

rm -rf ~/bigdata-project/logs/*
mkdir -p ~/bigdata-project/logs

echo ""
echo "====================================="
echo "✅ FULL RESET COMPLETE"
echo "====================================="
