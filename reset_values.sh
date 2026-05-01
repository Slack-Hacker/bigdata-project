<<<<<<< HEAD
#!/bin/bash
# ─────────────────────────────────────────────────────────
# reset_values.sh  —  Reset pipeline data safely
# (Kafka, Spark, HBase)
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "♻️ RESETTING PIPELINE DATA"
echo "====================================="

# ── 1. Clear application logs ───────────────────────────
echo ""
echo "1️⃣  Clearing application logs…"
rm -f ~/bigdata-project/logs/*.log

# ── 2. Reset Kafka topic (SAFE WAY) ─────────────────────
echo ""
echo "2️⃣  Resetting Kafka topic…"

~/kafka-zk/bin/kafka-topics.sh \
  --delete \
  --topic website_logs \
  --bootstrap-server master:9092 2>/dev/null || true

sleep 3

~/kafka-zk/bin/kafka-topics.sh \
  --create \
  --topic website_logs \
  --bootstrap-server master:9092 \
  --partitions 1 \
  --replication-factor 1

echo "   ✅ Kafka topic recreated"

# ── 3. Clear Spark checkpoints ──────────────────────────
echo ""
echo "3️⃣  Clearing Spark checkpoints…"

hdfs dfsadmin -safemode leave > /dev/null 2>&1 || true
hdfs dfs -rm -r -f /user/ankit/spark-checkpoints 2>/dev/null || true

echo "   ✅ Checkpoints cleared"

# ── 4. Reset HBase table ────────────────────────────────
echo ""
echo "4️⃣  Resetting HBase table…"

hbase shell <<'EOF'
disable 'web_traffic'
truncate 'web_traffic'
exit
EOF

echo "   ✅ HBase table reset"

echo ""
echo "====================================="
echo "✅ RESET COMPLETE"
echo "====================================="
=======
echo "clearing website logs"
rm -rf ~/website-traffic-monitoring-system/website.log

echo "clearing kafka_logs"
rm -rf ~/kafka-zk/kafka_logs

echo "clearing spark-checkpoints...."
hdfs dfs -rm -r /user/ankit/spark-checkpoints
>>>>>>> 3e8e9d96940598f46741697fb7af1a50fd08e4b3
