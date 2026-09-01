# 📊 Real-Time Website Traffic Monitoring System

[![Node.js](https://img.shields.io/badge/Node.js-18%2B-339933.svg)](https://nodejs.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.x-231F20.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C.svg)](https://spark.apache.org/)
[![Apache HBase](https://img.shields.io/badge/Apache_HBase-2.x-000000.svg)](https://hbase.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop-HDFS_YARN-66CCFF.svg)](https://hadoop.apache.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0-000000.svg)](https://flask.palletsprojects.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An enterprise-grade, distributed **Real-Time Website Traffic Monitoring System** built with **Node.js**, **Apache Kafka**, **Spark Structured Streaming**, **Apache HBase**, **Hadoop HDFS**, and **Flask**.

---

## 🌟 Key Resume Highlights & Technical Features

- **⚡ Clickstream Data Pipeline**: Built a real-time clickstream processing pipeline using **Node.js**, **Kafka**, **Spark Structured Streaming**, **HBase**, and **HDFS**.
- **📡 Kafka Event Ingestion**: Streamed page, action, timestamp, IP address, and user-agent events through **Apache Kafka** topics for real-time streaming processing.
- **⏱️ Spark Micro-Batch Processing**: Implemented **15-second Spark micro-batch aggregation** windows to process traffic throughput and active user metrics.
- **💾 Fault-Tolerant Storage**: Stored aggregated events in **Apache HBase** with **HDFS checkpoints** to enable stateful recovery and fault tolerance.
- **📈 Live Analytics Dashboard**: Developed a **Flask + Chart.js** web dashboard rendering live website traffic metrics, concurrent active users, and event analytics.
- **🌐 Distributed Cluster Infrastructure**: Configured a multi-node distributed cluster running **Hadoop HDFS**, **YARN**, **Apache Kafka**, **Spark**, and **HBase**.

---

## 🏗️ System Architecture

```
 ┌───────────────────┐    Kafka Event Producer     ┌───────────────────┐
 │ Node.js Web Server│ ──────────────────────────> │ Apache Kafka      │
 │ Clickstream Events│                             │ Ingestion Topics  │
 └───────────────────┘                             └─────────┬─────────┘
                                                             │
                                                             ▼
 ┌───────────────────┐    15s Micro-Batches        ┌───────────────────┐
 │ Flask Analytics   │ <────────────────────────── │ Spark Structured  │
 │ Live Dashboard    │                             │ Streaming Engine  │
 └───────────────────┘                             └─────────┬─────────┘
                                                             │
                                                             ▼
                                                   ┌───────────────────┐
                                                   │ Apache HBase      │
                                                   │ + HDFS Checkpoints│
                                                   └───────────────────┘
```

---

## 📁 Repository Structure

```
Real-Time-Website-traffic-Analysis/
├── producer/                 # Node.js clickstream event producer & simulator
├── streaming/                # PySpark / Scala Spark Structured Streaming jobs
├── storage/                  # Apache HBase schema scripts & HDFS configurations
├── dashboard/                # Flask web server & Chart.js live monitoring dashboard
├── cluster/                  # YARN, Hadoop, Kafka, and HBase cluster scripts
└── README.md                 # Project documentation
```

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for details.
