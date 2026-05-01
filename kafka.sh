#!/bin/bash
<<<<<<< HEAD
# ─────────────────────────────────────────────────────────
# kafka.sh — Start / Restart Kafka safely
# Usage:
#   ./kafka.sh           → SAFE restart
#   ./kafka.sh --reset   → FULL reset (deletes data)
# ─────────────────────────────────────────────────────────

echo "====================================="
echo "🚀 KAFKA MANAGER"
echo "====================================="

KAFKA_DIR=~/kafka-zk
LOG_DIR="$KAFKA_DIR/kafka-logs"

# ── 1. Stop Kafka ───────────────────────────────────────
echo ""
echo "1️⃣  Stopping Kafka…"
pkill -f kafka.Kafka 2>/dev/null || true
pkill -f kafka-server-start 2>/dev/null || true
sleep 3

# ── 2. Optional RESET ───────────────────────────────────
if [ "$1" == "--reset" ]; then
    echo ""
    echo "2️⃣  FULL RESET: Deleting Kafka logs…"
    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"
    echo "   ⚠️ All topics and data removed"
else
    echo ""
    echo "2️⃣  SAFE MODE: Keeping Kafka data"
fi

# ── 3. Start Kafka ──────────────────────────────────────
echo ""
echo "3️⃣  Starting Kafka…"
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
sleep 8

# ── 4. Verify ───────────────────────────────────────────
echo ""
echo "4️⃣  Checking Kafka status…"

if jps | grep -q Kafka; then
    echo "   ✅ Kafka is running"
else
    echo "   ❌ Kafka failed — check logs:"
    echo "   $KAFKA_DIR/logs/"
=======

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
>>>>>>> 3e8e9d96940598f46741697fb7af1a50fd08e4b3
fi

echo ""
echo "====================================="
<<<<<<< HEAD
echo "📨 Broker: master:9092"
echo "====================================="
=======
>>>>>>> 3e8e9d96940598f46741697fb7af1a50fd08e4b3
