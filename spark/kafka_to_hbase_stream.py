from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType
import requests
import happybase

# ---------------------------------------
# Spark Session
# ---------------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaToHBaseStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.streaming.backpressure.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------
# Read from Kafka
# ---------------------------------------
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

# ---------------------------------------
# Schema
# ---------------------------------------
schema = StructType([
    StructField("user_agent", StringType()),
    StructField("ip", StringType()),
    StructField("page", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType()),
])

parsed_df = (
    raw_df
    .select(from_json(col("value"), schema).alias("d"))
    .select("d.*")
    .filter(col("page").isNotNull())
)

# ---------------------------------------
# Aggregation
# ---------------------------------------
page_counts = parsed_df.groupBy("page").agg(
    count("*").alias("count"),
    spark_max("timestamp").alias("timestamp"),
    spark_max("user_agent").alias("user_agent"),
    spark_max("ip").alias("ip"),
    spark_max("action").alias("action"),
)

# ---------------------------------------
# Write to HBase
# ---------------------------------------
def write_to_hbase(batch_df, batch_id):

    rows = list(batch_df.toLocalIterator())
    if not rows:
        return

    try:
        connection = happybase.Connection(host="master", port=9090)
        table = connection.table("web_traffic")

        with table.batch(batch_size=500) as b:
            for row in rows:
                page = row["page"]

                b.put(
                    page.encode(),
                    {
                        b"cf:timestamp": (row["timestamp"] or "NA").encode(),
                        b"cf:page": page.encode(),
                        b"cf:user_agent": (row["user_agent"] or "NA").encode(),
                        b"cf:count": str(row["count"]).encode(),
                        b"cf:ip": (row["ip"] or "NA").encode(),
                        b"cf:action": (row["action"] or "visit").encode(),
                    }
                )

        connection.close()
        print(f"[HBase] Wrote {len(rows)} rows (batch {batch_id})")

    except Exception as e:
        print(f"[HBase ERROR] batch {batch_id}: {e}")

    # ---------------------------------------
    # Notify Dashboard
    # ---------------------------------------
    payload = [
        {
            "timestamp": row["timestamp"] or "NA",
            "page": row["page"],
            "user_agent": row["user_agent"] or "NA",
            "count": row["count"],
            "ip": row["ip"] or "NA",
            "action": row["action"] or "visit",
        }
        for row in rows
    ]

    try:
        requests.post("http://master:5000/update", json=payload, timeout=2)
    except Exception:
        print("[Dashboard] Server not reachable")

# ---------------------------------------
# Start Streaming
# ---------------------------------------
query = (
    page_counts.writeStream
    .outputMode("update")
    .foreachBatch(write_to_hbase)
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "hdfs://master:9000/user/ankit/spark-checkpoints")
    .start()
)

query.awaitTermination()
