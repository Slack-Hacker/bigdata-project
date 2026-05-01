#!/bin/bash
# ─────────────────────────────────────────────────────────
# ports.sh  —  Print all service URLs for this cluster
# ─────────────────────────────────────────────────────────

# Try enp0s8 first (VirtualBox host-only), fallback to any IP
MASTER_IP=$(ip -4 addr show enp0s8 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
if [ -z "$MASTER_IP" ]; then
    MASTER_IP=$(hostname -I | awk '{print $1}')
fi

echo "========================================"
echo " BIG DATA PROJECT — SERVICE URLS"
echo "========================================"
echo ""

echo "🗄️  Hadoop"
echo "  HDFS NameNode        →  http://$MASTER_IP:9870"
echo "  YARN ResourceManager →  http://$MASTER_IP:8088"
echo ""

echo "📡 Spark"
echo "  Spark Master UI      →  http://$MASTER_IP:8080"
echo "  Spark App UI         →  http://$MASTER_IP:4040"
echo ""

echo "🗃️  HBase"
echo "  HBase Master UI      →  http://$MASTER_IP:16010"
echo "  HBase Thrift (TCP)   →  $MASTER_IP:9090"
echo ""

echo "📨 Kafka"
echo "  Kafka Broker (TCP)   →  $MASTER_IP:9092"
echo ""

echo "🌐 Applications"
echo "  Analytics Dashboard  →  http://$MASTER_IP:5000"
echo "  Website              →  http://$MASTER_IP:3001"
echo ""

echo "========================================"
