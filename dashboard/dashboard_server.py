from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import happybase

app = Flask(__name__)
CORS(app)

# In-memory cache so the dashboard stays snappy even if HBase is slow
_cache = {}

# ─────────────────────────────────────────
# Helper
# ─────────────────────────────────────────
def read_hbase():
    """Scan web_traffic table and return aggregated metrics."""
    connection = happybase.Connection("master", port=9090)
    table = connection.table("web_traffic")

    logs          = []
    page_counts   = {}
    action_counts = {}
    user_counts   = {}
    total_visits  = 0

    for key, value in table.scan():
        page       = key.decode()
        ip         = value.get(b"cf:ip",         b"").decode()
        user_agent = value.get(b"cf:user_agent", b"").decode()
        action     = value.get(b"cf:action",     b"").decode()
        timestamp  = value.get(b"cf:timestamp",  b"").decode()
        cnt        = int(value.get(b"cf:count",  b"0").decode())

        logs.append({
            "page":       page,
            "ip":         ip,
            "user_agent": user_agent,
            "action":     action,
            "timestamp":  timestamp,
            "count":      cnt,
        })

        total_visits           += cnt
        page_counts[page]       = cnt
        action_counts[action]   = action_counts.get(action, 0) + cnt
        user_counts[user_agent] = user_counts.get(user_agent, 0) + cnt

    connection.close()

    return {
        "total_visits": total_visits,
        "pages":        page_counts,
        "actions":      action_counts,
        "users":        user_counts,
        "logs":         logs[-20:],
    }


# ─────────────────────────────────────────
# Routes
# ─────────────────────────────────────────
@app.route("/")
def home():
    return render_template("dashboard.html")


@app.route("/stats")
def stats():
    """Full stats read from HBase."""
    try:
        data = read_hbase()
        _cache.update(data)          # keep cache warm
        return jsonify(data)
    except Exception as e:
        # Fall back to cached data if HBase is temporarily unreachable
        if _cache:
            return jsonify(_cache)
        return jsonify({"error": str(e)}), 500


@app.route("/update", methods=["POST"])
def update():
    """
    Called by Spark after each micro-batch.
    Merges the incoming list of page records into the cache
    so the dashboard can refresh without hitting HBase every time.
    """
    records = request.get_json(silent=True) or []

    for rec in records:
        page = rec.get("page", "")
        if not page:
            continue

        cnt = rec.get("count", 0)

        # Update pages dict
        _cache.setdefault("pages", {})[page] = cnt

        # Update action counts
        action = rec.get("action", "visit")
        _cache.setdefault("actions", {})[action] = (
            _cache["actions"].get(action, 0) + cnt
        )

        # Update total
        _cache["total_visits"] = sum(_cache["pages"].values())

        # Append to logs (keep last 20)
        logs = _cache.setdefault("logs", [])
        logs.append(rec)
        _cache["logs"] = logs[-20:]

    return jsonify({"status": "ok"})


# ─────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
