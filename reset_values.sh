echo "clearing website logs"
rm -rf ~/website-traffic-monitoring-system/website.log

echo "clearing kafka_logs"
rm -rf ~/kafka-zk/kafka_logs

echo "clearing spark-checkpoints...."
hdfs dfs -rm -r /user/ankit/spark-checkpoints
