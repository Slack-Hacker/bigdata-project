from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
import happybase
import json
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ----------------------------
# Kafka Producer
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers='192.168.221.4:9092',   # change if needed
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ----------------------------
# TRACKING API
# ----------------------------
@app.route('/track', methods=['POST'])
def track():
    data = request.json

    event = {
        "user_agent": data.get("user_agent"),
        "ip": request.remote_addr,
        "page": data.get("page"),
        "action": data.get("action"),
        "timestamp": datetime.now().isoformat()
    }

    producer.send('website_logs', event)

    return jsonify({"status": "event sent"})


# ----------------------------
# DASHBOARD DATA API
# ----------------------------
@app.route('/data')
def get_data():
    connection = happybase.Connection('localhost')
    table = connection.table('web_traffic')

    result = {}

    for key, value in table.scan():
        page = key.decode()

        # Only read cf:count if exists
        if b'cf:count' in value:
            result[page] = int(value[b'cf:count'].decode())

    connection.close()
    return jsonify(result)

# ----------------------------
# START SERVER
# ----------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
