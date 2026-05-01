#!/bin/bash

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

echo ""
echo "====================================="
echo "🚀 STARTING BIG DATA PROJECT"
echo "====================================="

# ── 1. Hadoop ──────────────────────────────────────────
echo ""
echo "1️⃣  Starting HDFS + YARN…"
start-dfs.sh
start-yarn.sh
sleep 8

# ── 2. ZooKeeper ───────────────────────────────────────
echo ""
echo "2️⃣  Starting ZooKeeper…"
~/kafka-zk/bin/zookeeper-server-start.sh -daemon ~/kafka-zk/config/zookeeper.properties
sleep 8

# ── 3. Kafka ───────────────────────────────────────────
echo ""
echo "3️⃣  Starting Kafka…"
~/kafka-zk/bin/kafka-server-start.sh -daemon ~/kafka-zk/config/server.properties
sleep 10

echo "   Kafka status:"
jps | grep -i kafka || echo "   ⚠️ Kafka not detected"

# ── 4. HBase ───────────────────────────────────────────
echo ""
echo "4️⃣  Starting HBase…"
start-hbase.sh
sleep 15

# ── 5. HBase Thrift ────────────────────────────────────
echo ""
echo "5️⃣  Starting HBase Thrift…"
hbase-daemon.sh start thrift
sleep 5

# ── 6. Wait for HDFS ───────────────────────────────────
echo ""
echo "6️⃣  Waiting for HDFS Safe Mode OFF…"
hdfs dfsadmin -safemode leave > /dev/null 2>&1 || true

while true; do
    status=$(hdfs dfsadmin -safemode get 2>/dev/null)
    [[ "$status" == *"OFF"* ]] && break
    sleep 3
done

echo "   ✅ HDFS ready"

# ── 7. Spark Cluster (MASTER + WORKERS) ────────────────
echo ""
echo "7️⃣  Starting Spark Cluster…"
cd ~/spark
sbin/start-all.sh
sleep 5

echo "   Spark status:"
jps | grep -E "Master|Worker" || echo "   ⚠️ Spark not detected"

# ── 8. Spark Streaming Job ─────────────────────────────
echo ""
echo "8️⃣  Starting Spark Streaming…"
cd "$PROJECT_DIR"

spark-submit \
  --master spark://master:7077 \
  --conf spark.driver.host=172.21.134.147 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.port=4040 \
  --conf spark.port.maxRetries=0 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/kafka_to_hbase_stream.py > "$LOG_DIR/spark.log" 2>&1 &

sleep 5

# ── 9. Dashboard ───────────────────────────────────────
echo ""
echo "9️⃣  Starting Dashboard…"
cd "$PROJECT_DIR/dashboard"
python3 dashboard_server.py > "$LOG_DIR/dashboard.log" 2>&1 &

sleep 3

# ── 10. Website ────────────────────────────────────────
echo ""
echo "🔟  Starting Website…"
cd "$PROJECT_DIR/website-traffic-monitoring-system"
node server.js > "$LOG_DIR/website.log" 2>&1 &

# ── DONE ───────────────────────────────────────────────
echo ""
echo "====================================="
echo "✅ ALL SERVICES STARTED"
echo "====================================="
echo ""
echo "🌐 Website           →  http://master:3001"
echo "📊 Dashboard         →  http://master:5000"
echo "📡 Spark Master      →  http://master:8080"
echo "📈 Spark App UI      →  http://master:4040"
echo "🗄️ HDFS NameNode    →  http://master:9870"
echo "🎛️ YARN             →  http://master:8088"
echo "🏠 HBase Master      →  http://master:16010"
echo ""
echo "Logs → $LOG_DIR/"
echo "====================================="
