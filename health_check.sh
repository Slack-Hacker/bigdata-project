#!/bin/bash
# ─────────────────────────────────────────────────────────
# health_check.sh — Check status of all cluster services
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "🔍 CLUSTER HEALTH CHECK"
echo "====================================="

# ── JVM Services ───────────────────────
echo ""
echo "🧠 JVM SERVICES (jps)"
jps

# ── Hadoop / YARN ─────────────────────
echo ""
echo "🗄️ YARN NODES"
yarn node -list 2>/dev/null | grep RUNNING || echo "⚠️ YARN not running"

# ── Kafka ─────────────────────────────
echo ""
echo "📨 KAFKA TOPICS"

~/kafka-zk/bin/kafka-topics.sh \
--list \
--bootstrap-server master:9092 2>/dev/null \
|| echo "⚠️ Kafka not reachable"

# ── HBase ─────────────────────────────
echo ""
echo "🗃️ HBASE TABLES"

echo "list" | hbase shell -n 2>/dev/null | grep -v 'Took' \
|| echo "⚠️ HBase not responding"

# ── Ports Check ───────────────────────
echo ""
echo "🌐 PORT STATUS"

check_port () {
  port=$1
  name=$2
  if ss -tuln | grep -q ":$port"; then
    echo "✅ $name ($port) running"
  else
    echo "❌ $name ($port) NOT running"
  fi
}

check_port 9092 "Kafka"
check_port 5000 "Dashboard"
check_port 16010 "HBase UI"
check_port 8088 "YARN UI"
check_port 9870 "HDFS UI"

echo ""
echo "====================================="
echo "✅ HEALTH CHECK COMPLETE"
echo "====================================="
