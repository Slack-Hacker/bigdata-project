#!/bin/bash
# ─────────────────────────────────────────────────────────
# reset_cluster.sh  —  FULL RESET (cluster + data)
# Run AFTER stop_cluster.sh (or will stop everything here)
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "♻️ FULL CLUSTER RESET"
echo "====================================="

# ── 1. Stop all services ────────────────────────────────
echo ""
echo "1️⃣  Stopping all services…"

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
echo "2️⃣  Cleaning ZooKeeper data…"

rm -rf /home/ankit/zookeeper-data
rm -rf /tmp/zookeeper

# ── 3. Clean Kafka ──────────────────────────────────────
echo ""
echo "3️⃣  Cleaning Kafka logs…"

rm -rf ~/kafka-zk/kafka-logs
mkdir -p ~/kafka-zk/kafka-logs

# ── 4. Clean Spark ──────────────────────────────────────
echo ""
echo "4️⃣  Cleaning Spark temp + checkpoints…"

rm -rf /tmp/spark-*

# Restart HDFS briefly to delete checkpoint dir
start-dfs.sh > /dev/null 2>&1
sleep 10
hdfs dfsadmin -safemode leave > /dev/null 2>&1 || true
hdfs dfs -rm -r -f /user/ankit/spark-checkpoints 2>/dev/null || true
stop-dfs.sh > /dev/null 2>&1

# ── 5. Clean application logs ───────────────────────────
echo ""
echo "5️⃣  Cleaning application logs…"

rm -f ~/bigdata-project/logs/*.log

echo ""
echo "====================================="
echo "✅ FULL RESET COMPLETE"
echo "====================================="
