from flask import Flask, jsonify
from flask_cors import CORS
import happybase

app = Flask(__name__)
CORS(app)

@app.route("/stats")
def stats():

    connection = happybase.Connection('master', port=9090)
    table = connection.table('web_traffic')

    result = {}

    for key, value in table.scan():
        page = key.decode()

        if b'cf:count' in value:
            result[page] = int(value[b'cf:count'].decode())

    connection.close()

    return jsonify(result)

# Spark can still call this but we ignore it
@app.route("/update", methods=["POST"])
def update():
    return {"status": "ok"}

app.run(host="0.0.0.0", port=5000)

