from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType
import requests
import happybase

# ─────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("KafkaToHBaseStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.streaming.backpressure.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# Read from Kafka
# ─────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "master:9092")
    .option("subscribe", "website_logs")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING)")
)

# ─────────────────────────────────────────
# Schema & Parsing
# ─────────────────────────────────────────
schema = StructType([
    StructField("user_agent", StringType()),
    StructField("ip",         StringType()),
    StructField("page",       StringType()),
    StructField("action",     StringType()),
    StructField("timestamp",  StringType()),
])

parsed_df = (
    raw_df
    .select(from_json(col("value"), schema).alias("d"))
    .select("d.*")
    .filter(col("page").isNotNull())
)

# ─────────────────────────────────────────
# Aggregate: one row per page
# ─────────────────────────────────────────
page_counts = parsed_df.groupBy("page").agg(
    count("*").alias("count"),
    spark_max("timestamp").alias("timestamp"),
    spark_max("user_agent").alias("user_agent"),
    spark_max("ip").alias("ip"),
    spark_max("action").alias("action"),
)

# ─────────────────────────────────────────
# Write each micro-batch to HBase + notify dashboard
# ─────────────────────────────────────────
def write_to_hbase(batch_df, batch_id):
    rows = list(batch_df.toLocalIterator())
    if not rows:
        return

    # --- HBase write ---
    try:
        connection = happybase.Connection(host="master", port=9090)
        table = connection.table("web_traffic")

        with table.batch(batch_size=500) as b:
            for row in rows:
                page      = row["page"]
                cnt       = row["count"]
                timestamp = row["timestamp"]  or "NA"
                user_agent= row["user_agent"] or "NA"
                ip        = row["ip"]         or "NA"
                action    = row["action"]     or "visit"

                b.put(
                    page.encode(),
                    {
                        b"cf:timestamp":  timestamp.encode(),
                        b"cf:page":       page.encode(),
                        b"cf:user_agent": user_agent.encode(),
                        b"cf:count":      str(cnt).encode(),
                        b"cf:ip":         ip.encode(),
                        b"cf:action":     action.encode(),
                    }
                )

        connection.close()
        print(f"[HBase] Wrote {len(rows)} rows (batch {batch_id})")

    except Exception as e:
        print(f"[HBase ERROR] batch {batch_id}: {e}")

    # --- Notify dashboard ---
    payload = [
        {
            "timestamp":  row["timestamp"]  or "NA",
            "page":       row["page"],
            "user_agent": row["user_agent"] or "NA",
            "count":      row["count"],
            "ip":         row["ip"]         or "NA",
            "action":     row["action"]     or "visit",
        }
        for row in rows
    ]

    try:
        requests.post("http://master:5000/update", json=payload, timeout=2)
    except Exception:
        print("[Dashboard] Server not reachable — skipping push notification")


# ─────────────────────────────────────────
# Start Streaming
# ─────────────────────────────────────────
query = (
    page_counts.writeStream
    .outputMode("update")
    .foreachBatch(write_to_hbase)
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "hdfs://master:9000/user/ankit/spark-checkpoints")
    .start()
)

query.awaitTermination()
