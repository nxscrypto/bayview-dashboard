"""
app.py — Bayview Counseling Lead Dashboard
Serves a React dashboard that auto-refreshes from live Google Sheets data.

Run: python app.py
Prod: gunicorn app:app
"""

import json
import os
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, send_from_directory, render_template
from apscheduler.schedulers.background import BackgroundScheduler

from data_processor import generate_data

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bayview")

app = Flask(__name__, static_folder="static", template_folder="templates")

# ──────────────────────────────────────────────────────────────────────────────
# In-memory data cache
# ──────────────────────────────────────────────────────────────────────────────
_data_lock = threading.Lock()
_cached_data = None           # dict
_cached_json = None           # pre-serialized JSON string
_last_refresh = None          # datetime

REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "15"))


def refresh_data():
    """Fetch fresh data from Google Sheets and update the cache."""
    global _cached_data, _cached_json, _last_refresh
    logger.info("⟳  Refreshing data from Google Sheets …")
    try:
        data = generate_data()
        json_str = json.dumps(data, separators=(",", ":"))
        with _data_lock:
            _cached_data = data
            _cached_json = json_str
            _last_refresh = datetime.now()
        logger.info("✓  Data refreshed — %d bytes, %s leads",
                     len(json_str), data.get("all", {}).get("total", "?"))
    except Exception:
        logger.exception("✗  Data refresh failed")


def get_data():
    """Return cached data, refreshing if empty."""
    if _cached_json is None:
        refresh_data()
    return _cached_json, _last_refresh


# ──────────────────────────────────────────────────────────────────────────────
# Background scheduler — auto-refresh every N minutes
# ──────────────────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(refresh_data, "interval", minutes=REFRESH_MINUTES,
                  id="refresh", replace_existing=True)


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the dashboard."""
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/data")
def api_data():
    """Return the precomputed dashboard JSON."""
    data_json, refreshed = get_data()
    if data_json is None:
        return jsonify({"error": "Data not yet available, try again shortly"}), 503
    resp = app.response_class(response=data_json, status=200, mimetype="application/json")
    resp.headers["X-Last-Refresh"] = refreshed.isoformat() if refreshed else "never"
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Manually trigger a data refresh."""
    refresh_data()
    return jsonify({"ok": True, "refreshed": _last_refresh.isoformat() if _last_refresh else None})


@app.route("/api/status")
def api_status():
    """Health check."""
    return jsonify({
        "status": "ok",
        "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
        "total_leads": _cached_data.get("all", {}).get("total") if _cached_data else None,
        "refresh_interval_minutes": REFRESH_MINUTES,
    })


# ──────────────────────────────────────────────────────────────────────────────
# Startup
# ──────────────────────────────────────────────────────────────────────────────

def create_app():
    """Factory for gunicorn / production."""
    refresh_data()          # load data on startup
    scheduler.start()
    return app


if __name__ == "__main__":
    refresh_data()
    scheduler.start()
    port = int(os.environ.get("PORT", 5000))
    logger.info("Dashboard running on http://localhost:%d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
