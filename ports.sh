#!/bin/bash

echo "========================================"
echo " BIG DATA PROJECT - SERVICE DASHBOARD"
echo "========================================"
echo ""

MASTER_IP=$(ip -4 addr show enp0s8 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

echo "Cluster Node:"
echo "Master IP : $MASTER_IP"
echo ""

echo "----------------------------------------"
echo " Hadoop Services"
echo "----------------------------------------"

echo "HDFS NameNode UI:"
echo "http://$MASTER_IP:9870"

echo ""
echo "YARN ResourceManager UI:"
echo "http://$MASTER_IP:8088"

echo ""

echo "----------------------------------------"
echo " Spark"
echo "----------------------------------------"

echo "Spark Streaming UI:"
echo "http://$MASTER_IP:4040"

echo ""

echo "----------------------------------------"
echo " HBase"
echo "----------------------------------------"

echo "HBase Master UI:"
echo "http://$MASTER_IP:16010"

echo ""

echo "----------------------------------------"
echo " Kafka"
echo "----------------------------------------"

echo "Kafka Broker:"
echo "$MASTER_IP:9092"

echo ""

echo "----------------------------------------"
echo " Project Dashboard"
echo "----------------------------------------"

echo "Analytics Dashboard:"
echo "http://$MASTER_IP:5000"

echo ""

echo "----------------------------------------"
echo " Website"
echo "----------------------------------------"

echo "Website:"
echo "http://$MASTER_IP:3001"

echo ""

echo "========================================"
echo "Use these URLs in your browser"
echo "========================================"
