# Big Data Website Traffic Monitoring

Real-time website traffic pipeline:
**Node.js → Kafka → Spark Streaming → HBase → Flask Dashboard**

## Architecture

```
Browser
  └─► Node.js server (port 3001)
        └─► Kafka topic: website_logs
              └─► Spark Structured Streaming
                    └─► HBase table: web_traffic
                          └─► Flask dashboard (port 5000)
```

## Prerequisites (already installed)

| Tool | Version |
|------|---------|
| Hadoop (HDFS + YARN) | 3.x |
| Apache Kafka | any |
| Apache HBase | 2.x |
| Apache Spark | 3.5 |
| Python 3 + pip | — |
| Node.js + npm | — |

Python packages required:
```
pip install flask flask-cors happybase
```

## Quick Start

```bash
# 1. Start all services + pipeline
cd ~/bigdata-project
bash run_project.sh

# 2. (First run only) create Kafka topic & HBase table
bash config/kafka_topic.sh
bash config/create_hbase_table.sh
```

## Scripts

| Script | Purpose |
|--------|---------|
| `run_project.sh` | Start everything (Hadoop → Kafka → HBase → Spark → Dashboard → Website) |
| `stop_cluster.sh` | Gracefully stop all services |
| `reset_cluster.sh` | Full reset — clears data, logs, checkpoints, then stops |
| `reset_values.sh` | Clear live data only (HBase + checkpoints) while services stay up |
| `health_check.sh` | Show JVM procs, YARN nodes, Kafka topics, HBase tables |
| `ports.sh` | Print all service URLs |
| `kafka.sh` | Restart Kafka only |

## Project Structure

```
bigdata-project/
├── spark/
│   └── kafka_to_hbase_stream.py   # Spark Structured Streaming job
├── dashboard/
│   ├── dashboard_server.py        # Flask API + /stats + /update
│   └── templates/
│       └── dashboard.html         # Dark-theme analytics UI
├── website-traffic-monitoring-system/
│   ├── server.js                  # Express + KafkaJS producer
│   └── public/                    # Static HTML pages being tracked
├── config/
│   ├── kafka_topic.sh
│   └── create_hbase_table.sh
├── logs/                          # Created at runtime
├── run_project.sh
├── stop_cluster.sh
├── reset_cluster.sh
├── reset_values.sh
├── health_check.sh
├── ports.sh
└── kafka.sh
```

## Dashboard

Open **http://master:5000** (or your master IP) for the real-time analytics dashboard:

- KPI cards: total visits, unique pages, action types, unique agents
- Bar chart: page traffic distribution
- Doughnut chart: action breakdown
- Live log table: last 20 events, auto-refreshes every 3 s
