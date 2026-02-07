"""
app.py — Bayview Counseling Lead Dashboard
Serves a React dashboard that auto-refreshes from live Google Sheets data.
"""

import json
import os
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, send_from_directory
from apscheduler.schedulers.background import BackgroundScheduler

from data_processor import generate_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bayview")

app = Flask(__name__, static_folder="static")

# In-memory data cache
_data_lock = threading.Lock()
_cached_data = None
_cached_json = None
_last_refresh = None
_initial_load_started = False

REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "15"))


def refresh_data():
    global _cached_data, _cached_json, _last_refresh
    logger.info("Refreshing data from Google Sheets...")
    try:
        data = generate_data()
        json_str = json.dumps(data, separators=(",", ":"))
        with _data_lock:
            _cached_data = data
            _cached_json = json_str
            _last_refresh = datetime.now()
        logger.info("Data refreshed — %d bytes", len(json_str))
    except Exception:
        logger.exception("Data refresh failed")


def ensure_data_loaded():
    global _initial_load_started
    if _cached_json is None and not _initial_load_started:
        _initial_load_started = True
        threading.Thread(target=refresh_data, daemon=True).start()


# Background scheduler
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(refresh_data, "interval", minutes=REFRESH_MINUTES,
                  id="refresh", replace_existing=True)


@app.route("/")
def index():
    ensure_data_loaded()
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/data")
def api_data():
    ensure_data_loaded()
    if _cached_json is None:
        return jsonify({"error": "Data loading, please refresh in a few seconds"}), 503
    resp = app.response_class(response=_cached_json, status=200, mimetype="application/json")
    resp.headers["X-Last-Refresh"] = _last_refresh.isoformat() if _last_refresh else "never"
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    refresh_data()
    return jsonify({"ok": True, "refreshed": _last_refresh.isoformat() if _last_refresh else None})


@app.route("/api/status")
def api_status():
    return jsonify({
        "status": "ok",
        "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
        "total_leads": _cached_data.get("all", {}).get("total") if _cached_data else None,
        "refresh_interval_minutes": REFRESH_MINUTES,
    })


# Start scheduler on import (but don't block fetching data)
scheduler.start()
logger.info("Bayview Dashboard started — scheduler running every %d min", REFRESH_MINUTES)


if __name__ == "__main__":
    refresh_data()
    port = int(os.environ.get("PORT", 5000))
    logger.info("Dashboard running on http://localhost:%d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
