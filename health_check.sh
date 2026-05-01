#!/bin/bash
# ─────────────────────────────────────────────────────────
# health_check.sh  —  Full system health overview
# ─────────────────────────────────────────────────────────

echo "========================================"
echo "🩺 BIG DATA PIPELINE HEALTH CHECK"
echo "========================================"

# ── 1. JVM Services ─────────────────────
echo ""
echo "🔹 JVM SERVICES"
jps

# ── 2. Spark Cluster ────────────────────
echo ""
echo "🔹 SPARK CLUSTER"
jps | grep -E "Master|Worker" || echo "(Spark not running)"

# ── 3. YARN ─────────────────────────────
echo ""
echo "🔹 YARN NODES"
yarn node -list 2>/dev/null || echo "(YARN not running)"

# ── 4. Kafka ────────────────────────────
echo ""
echo "🔹 KAFKA TOPICS"
~/kafka-zk/bin/kafka-topics.sh \
  --list \
  --bootstrap-server master:9092 2>/dev/null || echo "(Kafka not running)"

# ── 5. Kafka Test (last message) ────────
echo ""
echo "🔹 KAFKA SAMPLE MESSAGE"
timeout 3 ~/kafka-zk/bin/kafka-console-consumer.sh \
  --bootstrap-server master:9092 \
  --topic website_logs \
  --from-beginning \
  --max-messages 1 2>/dev/null || echo "(no messages)"

# ── 6. HBase ────────────────────────────
echo ""
echo "🔹 HBASE TABLES"
echo "list" | hbase shell -n 2>/dev/null || echo "(HBase not running)"

# ── 7. App Processes ────────────────────
echo ""
echo "🔹 APPLICATION PROCESSES"
pgrep -a python3 | grep dashboard_server || echo "(Dashboard not running)"
pgrep -a node    | grep server.js        || echo "(Website not running)"

# ── 8. Port Checks ──────────────────────
echo ""
echo "🔹 PORT STATUS"

check_port () {
    PORT=$1
    NAME=$2
    if ss -tuln | grep -q ":$PORT"; then
        echo "   ✅ $NAME ($PORT)"
    else
        echo "   ❌ $NAME ($PORT)"
    fi
}

check_port 8080 "Spark Master"
check_port 4040 "Spark App UI"
check_port 9092 "Kafka"
check_port 5000 "Dashboard"
check_port 3001 "Website"
check_port 16010 "HBase Master"
check_port 9870 "HDFS"
check_port 8088 "YARN"

echo ""
echo "========================================"
echo "✅ HEALTH CHECK COMPLETE"
echo "========================================"
