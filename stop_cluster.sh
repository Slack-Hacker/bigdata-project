#!/bin/bash
# ─────────────────────────────────────────────────────────
# stop_cluster.sh  —  Gracefully stop all services
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "🛑 STOPPING BIG DATA CLUSTER"
echo "====================================="

echo ""
echo "1️⃣  Stopping Spark Streaming jobs…"
pkill -f spark-submit 2>/dev/null || true
sleep 2

echo ""
echo "2️⃣  Stopping Spark Cluster (Master + Workers)…"
cd ~/spark 2>/dev/null || true
sbin/stop-all.sh 2>/dev/null || true
sleep 3

echo ""
echo "3️⃣  Stopping Dashboard & Website…"
pkill -f dashboard_server.py 2>/dev/null || true
pkill -f "node server.js"    2>/dev/null || true
sleep 2

echo ""
echo "4️⃣  Stopping HBase…"
stop-hbase.sh 2>/dev/null || true
hbase-daemon.sh stop thrift 2>/dev/null || true
sleep 5

echo ""
echo "5️⃣  Stopping Kafka…"
pkill -f kafka.Kafka        2>/dev/null || true
pkill -f kafka-server-start 2>/dev/null || true
sleep 2

echo ""
echo "6️⃣  Stopping ZooKeeper…"
pkill -f QuorumPeerMain 2>/dev/null || true
sleep 2

echo ""
echo "7️⃣  Stopping YARN…"
stop-yarn.sh 2>/dev/null || true
sleep 3

echo ""
echo "8️⃣  Stopping HDFS…"
stop-dfs.sh 2>/dev/null || true
sleep 3

echo ""
echo "9️⃣  Cleaning temp files…"
rm -rf /tmp/hbase-* /tmp/hsperfdata_* /tmp/spark-*

echo ""
echo "====================================="
echo "✅ CLUSTER STOPPED CLEANLY"
echo "====================================="
