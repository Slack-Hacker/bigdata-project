# 📊 Real-Time Website Traffic Analytics System

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-Streaming-E25A1C.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Streams-231F20.svg)](https://kafka.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A enterprise-grade **Real-Time Distributed Traffic Monitoring & Big Data Analytics Platform** built with **Apache Spark Streaming**, **Apache Kafka**, **Python**, and interactive web dashboards.

The platform ingests high-throughput website event streams, processes real-time metrics (unique visitors, page views, geographic distribution, bounce rates), and visualizes live analytics on dynamic dashboards.

---

## 🌟 Key Capabilities

- **⚡ High-Throughput Event Ingestion**: Ingests live web telemetry event streams using Apache Kafka topics.
- **🔥 Distributed Real-Time Processing**: Computes streaming aggregations, windowed metrics, and anomaly detection using Apache Spark Structured Streaming.
- **📈 Interactive Live Dashboard**: Web-based monitoring frontend for visualizing active user sessions, top requested URLs, traffic spikes, and server health.
- **🛠️ Automated Cluster Lifecycle Scripts**: One-click cluster initialization, streaming startup, health checks, and shutdown scripts (`start_cluster.sh`, `start_streaming.sh`, `health_check.sh`).

---

## 📁 Repository Structure

```
Real-Time-Website-traffic-Analysis/
├── spark/                # Apache Spark Structured Streaming jobs & aggregation pipelines
├── config/ & configs/    # Kafka brokers, Zookeeper & Spark cluster configurations
├── backend/              # Python API backend for serving streaming analytics metrics
├── dashboard/            # Web analytics dashboard user interface
├── screenshots/          # Platform preview screenshots
├── start_cluster.sh      # Cluster startup & services launcher
├── start_streaming.sh    # Launches Spark streaming streaming jobs
├── health_check.sh       # System status & port monitoring script
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation
```

---

## 🚀 Quick Start & Deployment

### Prerequisites

- Apache Kafka & Zookeeper
- Apache Spark 3.x
- Python 3.9+

### 1. Clone the Repository

```bash
git clone https://github.com/Slack-Hacker/Real-Time-Website-traffic-Analysis.git
cd Real-Time-Website-traffic-Analysis
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Launch Cluster & Streaming Pipeline

```bash
# 1. Start Zookeeper & Kafka brokers
./start_cluster.sh

# 2. Launch Spark Structured Streaming pipeline
./start_streaming.sh

# 3. Verify health status
./health_check.sh
```

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for details.
