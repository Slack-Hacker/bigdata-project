from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import happybase

app = Flask(__name__)
CORS(app)

_cache = {
    "total_visits": 0,
    "pages": {},
    "actions": {},
    "users": {},
    "logs": []
}

def read_hbase():
    connection = happybase.Connection("master", port=9090)
    table = connection.table("web_traffic")

    logs = []
    page_counts = {}
    action_counts = {}
    user_counts = {}
    total_visits = 0

    for key, value in table.scan():
        page = key.decode()
        cnt = int(value.get(b"cf:count", b"0").decode())
        action = value.get(b"cf:action", b"visit").decode()
        user = value.get(b"cf:user_agent", b"unknown").decode()
        ip = value.get(b"cf:ip", b"").decode()
        timestamp = value.get(b"cf:timestamp", b"").decode()

        logs.append({
            "page": page,
            "ip": ip,
            "user_agent": user,
            "action": action,
            "timestamp": timestamp,
            "count": cnt
        })

        total_visits += cnt
        page_counts[page] = cnt
        action_counts[action] = action_counts.get(action, 0) + cnt
        user_counts[user] = user_counts.get(user, 0) + cnt

    connection.close()

    return {
        "total_visits": total_visits,
        "pages": page_counts,
        "actions": action_counts,
        "users": user_counts,
        "logs": logs[-20:]
    }

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/stats")
def stats():
    try:
        data = read_hbase()
        _cache.update(data)
        return jsonify(data)
    except:
        return jsonify(_cache)

@app.route("/update", methods=["POST"])
def update():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
