from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import requests
import happybase

# ---------------------------------------
# Create Spark Session
# ---------------------------------------
spark = SparkSession.builder \
    .appName("KafkaToHBaseStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------
# Read from Kafka
# ---------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.221.4:9092") \
    .option("subscribe", "website_logs") \
    .option("failOnDataLoss","false") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to STRING
df = df.selectExpr("CAST(value AS STRING)")

# ---------------------------------------
# Define Schema (NO status field)
# ---------------------------------------
schema = StructType([
    StructField("user_agent", StringType()),
    StructField("ip", StringType()),
    StructField("page", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType())
])

parsed_df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.filter(col("page").isNotNull())

# ---------------------------------------
# Aggregate only by PAGE
# ---------------------------------------
page_counts = parsed_df.groupBy("page").count()

# ---------------------------------------
# Write to HBase
# ---------------------------------------

def write_to_hbase(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    rows = batch_df.collect()

    # -------- Write to HBase --------
    connection = happybase.Connection(
        host='master',
        port=9090
    )

    table = connection.table('web_traffic')

    with table.batch(batch_size=100) as b:
        for row in rows:

            page = row['page']
            count = row['count']

#            existing = table.row(page.encode())
#
#            if b'cf:count' in existing:
#                old_count = int(existing[b'cf:count'].decode())
#            else:
#                old_count = 0

#            total = old_count + count
            total = count + 1

            b.put(
                page.encode(),
                {
                    b'cf:count': str(total).encode()
                }
            )

    connection.close()

    # -------- Send data to Dashboard --------
    data = []

    for row in rows:
        data.append({
            "page": row['page'],
            "count": row['count']
        })

    try:
        requests.post(
            "http://master:5000/update",
            json=data,
            timeout=3
        )
    except:
        print("Dashboard server not reachable")

#----------------------------------------
query = page_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_hbase) \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation","hdfs://master:9000/user/ankit/spark-checkpoints") \
    .start()

query.awaitTermination()
