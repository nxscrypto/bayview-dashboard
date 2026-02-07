"""
app.py — Bayview Counseling Lead Dashboard
Uses Redis to cache data so restarts are instant.
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

# ── Redis setup ──────────────────────────────────────────────────────────────
_redis = None
REDIS_KEY = "bayview:dashboard_data"
REDIS_TS_KEY = "bayview:last_refresh"

def get_redis():
    global _redis
    if _redis is None:
        redis_url = os.environ.get("REDIS_URL")
        if redis_url:
            try:
                import redis
                _redis = redis.from_url(redis_url, decode_responses=True)
                _redis.ping()
                logger.info("Redis connected")
            except Exception as e:
                logger.warning("Redis not available: %s", e)
                _redis = False  # Mark as unavailable
        else:
            _redis = False
    return _redis if _redis else None


# ── In-memory cache ──────────────────────────────────────────────────────────
_data_lock = threading.Lock()
_cached_data = None
_cached_json = None
_last_refresh = None
_initial_load_started = False

REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "15"))


def save_to_redis(json_str):
    r = get_redis()
    if r:
        try:
            r.set(REDIS_KEY, json_str)
            r.set(REDIS_TS_KEY, datetime.now().isoformat())
            logger.info("Saved to Redis cache")
        except Exception as e:
            logger.warning("Redis save failed: %s", e)


def load_from_redis():
    r = get_redis()
    if r:
        try:
            data = r.get(REDIS_KEY)
            ts = r.get(REDIS_TS_KEY)
            if data:
                logger.info("Loaded from Redis cache (%d bytes)", len(data))
                return data, ts
        except Exception as e:
            logger.warning("Redis load failed: %s", e)
    return None, None


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
        save_to_redis(json_str)
        logger.info("Data refreshed — %d bytes", len(json_str))
    except Exception:
        logger.exception("Data refresh failed")


def ensure_data_loaded():
    global _cached_json, _cached_data, _last_refresh, _initial_load_started
    if _cached_json is not None:
        return
    if not _initial_load_started:
        _initial_load_started = True
        # Try Redis first (instant)
        cached, ts = load_from_redis()
        if cached:
            with _data_lock:
                _cached_json = cached
                _cached_data = json.loads(cached)
                _last_refresh = datetime.fromisoformat(ts) if ts else datetime.now()
            # Still refresh from Sheets in background
            threading.Thread(target=refresh_data, daemon=True).start()
            return
        # No Redis cache, fetch from Sheets in background
        threading.Thread(target=refresh_data, daemon=True).start()


# ── Background scheduler ────────────────────────────────────────────────────
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(refresh_data, "interval", minutes=REFRESH_MINUTES,
                  id="refresh", replace_existing=True)


# ── Routes ───────────────────────────────────────────────────────────────────

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


# ── Startup ──────────────────────────────────────────────────────────────────
scheduler.start()
logger.info("Bayview Dashboard started — scheduler every %d min", REFRESH_MINUTES)
# Pre-load from Redis on startup
ensure_data_loaded()


if __name__ == "__main__":
    refresh_data()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
