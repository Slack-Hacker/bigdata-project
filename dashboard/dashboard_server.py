from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import happybase

app = Flask(__name__)
CORS(app)

# In-memory cache for faster dashboard updates
_cache = {
    "total_visits": 0,
    "pages": {},
    "actions": {},
    "users": {},
    "logs": []
}

# ----------------------------------------
# Read from HBase
# ----------------------------------------
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

        ip = value.get(b"cf:ip", b"").decode()
        user_agent = value.get(b"cf:user_agent", b"").decode()
        action = value.get(b"cf:action", b"visit").decode()
        timestamp = value.get(b"cf:timestamp", b"").decode()
        cnt = int(value.get(b"cf:count", b"0").decode())

        logs.append({
            "page": page,
            "ip": ip,
            "user_agent": user_agent,
            "action": action,
            "timestamp": timestamp,
            "count": cnt,
        })

        total_visits += cnt
        page_counts[page] = cnt
        action_counts[action] = action_counts.get(action, 0) + cnt
        user_counts[user_agent] = user_counts.get(user_agent, 0) + cnt

    connection.close()

    return {
        "total_visits": total_visits,
        "pages": page_counts,
        "actions": action_counts,
        "users": user_counts,
        "logs": logs[-20:],  # last 20 logs
    }

# ----------------------------------------
# Routes
# ----------------------------------------

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/stats")
def stats():
    try:
        data = read_hbase()
        _cache.update(data)
        return jsonify(data)   # IMPORTANT: full data
    except Exception as e:
        print("[HBase ERROR]", e)

        # fallback to cache
        return jsonify(_cache)

@app.route("/update", methods=["POST"])
def update():
    records = request.get_json(silent=True) or []

    for rec in records:
        page = rec.get("page")
        if not page:
            continue

        cnt = rec.get("count", 0)
        action = rec.get("action", "visit")
        user = rec.get("user_agent", "unknown")

        # update pages
        _cache.setdefault("pages", {})[page] = cnt

        # update actions
        _cache.setdefault("actions", {})[action] = \
            _cache["actions"].get(action, 0) + cnt

        # update users
        _cache.setdefault("users", {})[user] = \
            _cache["users"].get(user, 0) + cnt

        # update total
        _cache["total_visits"] = sum(_cache["pages"].values())

        # update logs
        logs = _cache.setdefault("logs", [])
        logs.append(rec)
        _cache["logs"] = logs[-20:]

    return jsonify({"status": "ok"})

# ----------------------------------------
# Start server
# ----------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
