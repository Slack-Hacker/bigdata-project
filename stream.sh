#!/bin/bash
# ----------------------------------------
# stream.sh
# Restart Spark Streaming job
# Usage:
#   ./stream.sh          → normal restart
#   ./stream.sh --reset  → reset checkpoints
# ----------------------------------------

PROJECT_DIR="$HOME/bigdata-project"
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"

echo "====================================="
echo "🔄 Restarting Spark Streaming Job"
echo "====================================="

# ── 1. Stop old streaming job ───────────
echo ""
echo "1️⃣  Stopping old Spark job…"
pkill -f kafka_to_hbase_stream.py 2>/dev/null || true
sleep 3

# ── 2. Optional checkpoint reset ────────
if [ "$1" == "--reset" ]; then
    echo ""
    echo "2️⃣  Resetting Spark checkpoints…"
    hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
    hdfs dfs -rm -r -f /user/ankit/spark-checkpoints >/dev/null 2>&1 || true
    echo "   ✅ Checkpoints cleared"
else
    echo ""
    echo "2️⃣  Keeping existing checkpoints"
fi

# ── 3. Start streaming job ──────────────
echo ""
echo "3️⃣  Starting Spark Streaming…"

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

# ── 4. Verify ──────────────────────────
echo ""
echo "4️⃣  Checking status…"

if jps | grep -q SparkSubmit; then
    echo "   ✅ Spark Streaming is running"
else
    echo "   ❌ Spark failed — check logs"
fi

echo ""
echo "====================================="
echo "📄 Logs: $LOG_DIR/spark.log"
echo "📡 Spark Master UI: http://master:8080"
echo "📈 Spark App UI:   http://master:4040"
echo "====================================="
