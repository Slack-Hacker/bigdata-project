#!/bin/bash

# ─────────────────────────────────────────────────────────
# stop_cluster.sh  —  Gracefully stop all services
# ─────────────────────────────────────────────────────────

set +e  # don't exit on errors while stopping

echo "====================================="
echo "🛑 STOPPING BIG DATA CLUSTER"
echo "====================================="

# ── 1. Spark Streaming Jobs ─────────────────────────────
echo ""
echo "1️⃣  Stopping Spark Streaming jobs…"
pkill -f spark-submit 2>/dev/null || true
sleep 2

# ── 2. Spark Cluster ───────────────────────────────────
echo ""
echo "2️⃣  Stopping Spark Cluster (Master + Workers)…"
cd ~/spark 2>/dev/null || true
sbin/stop-all.sh 2>/dev/null || true
sleep 3

# ── 3. Dashboard & Website ─────────────────────────────
echo ""
echo "3️⃣  Stopping Dashboard & Website…"
pkill -f dashboard_server.py 2>/dev/null || true
pkill -f "node server.js"    2>/dev/null || true
sleep 2

# ── 4. HBase ───────────────────────────────────────────
echo ""
echo "4️⃣  Stopping HBase…"
hbase-daemon.sh stop thrift 2>/dev/null || true
stop-hbase.sh 2>/dev/null || true
sleep 5

# ── 5. Kafka ───────────────────────────────────────────
echo ""
echo "5️⃣  Stopping Kafka…"
pkill -f kafka.Kafka        2>/dev/null || true
pkill -f kafka-server-start 2>/dev/null || true
sleep 2

# ── 6. ZooKeeper ───────────────────────────────────────
echo ""
echo "6️⃣  Stopping ZooKeeper…"
pkill -f QuorumPeerMain 2>/dev/null || true
sleep 2

# ── 7. YARN ────────────────────────────────────────────
echo ""
echo "7️⃣  Stopping YARN…"
stop-yarn.sh 2>/dev/null || true
sleep 3

# ── 8. HDFS ────────────────────────────────────────────
echo ""
echo "8️⃣  Stopping HDFS…"
stop-dfs.sh 2>/dev/null || true
sleep 3

# ── 9. Cleanup ─────────────────────────────────────────
echo ""
echo "9️⃣  Cleaning temp files…"
rm -rf /tmp/hbase-* /tmp/hsperfdata_* /tmp/spark-* 2>/dev/null || true

echo ""
echo "====================================="
echo "✅ CLUSTER STOPPED CLEANLY"
echo "====================================="
