"""
app.py — Bayview Counseling Lead Dashboard
"""

import json
import os
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify, send_from_directory, request
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("bayview")

app = Flask(__name__, static_folder="static")

# ── Database ─────────────────────────────────────────────────────────────────
from database import init_db, add_lead, update_lead, get_pending_leads, get_recent_leads, get_lead, delete_lead
from database import init_rental_db, add_rental_entry, add_rental_entries_bulk, update_rental_entry, delete_rental_entry, get_rental_entry, get_rental_entries_by_week, get_recent_rental_entries, get_rental_weeks, delete_rental_week
from calendar_sync import get_sessions_data
init_db()
init_rental_db()

# ── State ────────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_data_json = None
_data_dict = None
_last_refresh = None
_loading = False

REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "15"))


# ── Redis helpers ────────────────────────────────────────────────────────────
def _get_redis():
    url = os.environ.get("REDIS_URL")
    if not url:
        logger.info("No REDIS_URL set")
        return None
    try:
        import redis as r
        client = r.from_url(url, decode_responses=True, socket_timeout=5)
        client.ping()
        return client
    except Exception as e:
        logger.warning("Redis unavailable: %s", e)
        return None


def _save_redis(json_str):
    try:
        client = _get_redis()
        if client:
            client.set("bayview:data", json_str)
            client.set("bayview:ts", datetime.now().isoformat())
            logger.info("Saved %d bytes to Redis", len(json_str))
    except Exception as e:
        logger.warning("Redis save error: %s", e)


def _load_redis():
    try:
        client = _get_redis()
        if client:
            data = client.get("bayview:data")
            ts = client.get("bayview:ts")
            if data:
                logger.info("Loaded %d bytes from Redis", len(data))
                return data, ts
    except Exception as e:
        logger.warning("Redis load error: %s", e)
    return None, None


# ── Data loading ─────────────────────────────────────────────────────────────
def _do_refresh():
    global _data_json, _data_dict, _last_refresh, _loading
    logger.info("Fetching data from Google Sheets...")
    try:
        from data_processor import generate_data
        data = generate_data()
        js = json.dumps(data, separators=(",", ":"))
        with _lock:
            _data_json = js
            _data_dict = data
            _last_refresh = datetime.now()
            _loading = False
        _save_redis(js)
        logger.info("Data ready — %d bytes, %s leads",
                     len(js), data.get("all", {}).get("total", "?"))
    except Exception:
        logger.exception("Refresh failed")
        with _lock:
            _loading = False


def _ensure_loaded():
    global _data_json, _data_dict, _last_refresh, _loading
    with _lock:
        if _data_json is not None or _loading:
            return
        _loading = True

    # Try Redis first
    cached, ts = _load_redis()
    if cached:
        with _lock:
            _data_json = cached
            _data_dict = json.loads(cached)
            _last_refresh = datetime.fromisoformat(ts) if ts else datetime.now()
            _loading = False
        logger.info("Serving from Redis cache")
        # Still refresh in background
        threading.Thread(target=_do_refresh, daemon=True).start()
        return

    # No cache — fetch in background
    threading.Thread(target=_do_refresh, daemon=True).start()


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    _ensure_loaded()
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/data")
def api_data():
    _ensure_loaded()
    with _lock:
        js = _data_json
        ts = _last_refresh

    if js is None:
        return jsonify({"error": "Still loading data, retry in a few seconds"}), 503

    resp = app.response_class(response=js, status=200, mimetype="application/json")
    resp.headers["X-Last-Refresh"] = ts.isoformat() if ts else "never"
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=_do_refresh, daemon=True).start()
    return jsonify({"ok": True, "refreshed": _last_refresh.isoformat() if _last_refresh else None})


@app.route("/api/status")
def api_status():
    return jsonify({"status": "ok", "loaded": _data_json is not None})



# ── Lead API ─────────────────────────────────────────────────────────────────
@app.route("/api/leads", methods=["POST"])
def api_add_lead():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    required = ["date", "location", "first_name", "last_name", "phone",
                 "service_type", "presenting_problem", "referral_source",
                 "action_taken", "referred_to", "referral_outcome"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400
    lead_id = add_lead(data)
    return jsonify({"ok": True, "id": lead_id}), 201


@app.route("/api/leads/pending")
def api_pending_leads():
    days = request.args.get("days", 14, type=int)
    leads = get_pending_leads(days)
    return jsonify(leads)



@app.route("/api/leads/recent")
def api_recent_leads():
    days = request.args.get("days", 30, type=int)
    leads = get_recent_leads(days)
    return jsonify(leads)

@app.route("/api/leads/<int:lead_id>", methods=["PUT"])
def api_update_lead(lead_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    lead = get_lead(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404
    update_lead(lead_id, data)
    updated = get_lead(lead_id)
    return jsonify({"ok": True, "lead": updated})


@app.route("/api/leads/<int:lead_id>")
def api_get_lead(lead_id):
    lead = get_lead(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404
    return jsonify(lead)

@app.route("/api/leads/<int:lead_id>", methods=["DELETE"])
def api_delete_lead(lead_id):
    lead = get_lead(lead_id)
    if not lead:
        return jsonify({"error": "Lead not found"}), 404
    delete_lead(lead_id)
    return jsonify({"ok": True, "deleted": lead_id})




# ── Rental API ───────────────────────────────────────────────────────────────────────

@app.route("/api/rental", methods=["POST"])
def api_add_rental():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Support bulk insert (list of entries) or single entry
    entries = data.get("entries")
    if entries and isinstance(entries, list):
        # Bulk insert for a whole week
        required = ["week_start", "week_end"]
        for field in required:
            if not data.get(field):
                return jsonify({"error": f"Missing: {field}"}), 400

        week_start = data["week_start"]
        week_end = data["week_end"]
        bulk = []
        for e in entries:
            if not e.get("therapist") or not e.get("amount"):
                continue
            bulk.append({
                "week_start": week_start,
                "week_end": week_end,
                "therapist": e["therapist"],
                "location": e.get("location", ""),
                "amount": e["amount"],
                "category": e.get("category", "room_rental"),
                "notes": e.get("notes", ""),
            })
        if not bulk:
            return jsonify({"error": "No valid entries"}), 400
        ids = add_rental_entries_bulk(bulk)
        return jsonify({"ok": True, "ids": ids, "count": len(ids)}), 201
    else:
        # Single entry
        required = ["week_start", "week_end", "therapist", "amount"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400
        entry_id = add_rental_entry(data)
        return jsonify({"ok": True, "id": entry_id}), 201


@app.route("/api/rental/week")
def api_rental_by_week():
    week_start = request.args.get("week_start")
    week_end = request.args.get("week_end")
    if not week_start:
        return jsonify({"error": "week_start required"}), 400
    entries = get_rental_entries_by_week(week_start, week_end)
    return jsonify(entries)


@app.route("/api/rental/recent")
def api_recent_rental():
    weeks = request.args.get("weeks", 12, type=int)
    entries = get_recent_rental_entries(weeks)
    return jsonify(entries)


@app.route("/api/rental/weeks")
def api_rental_weeks():
    weeks = get_rental_weeks()
    return jsonify(weeks)


@app.route("/api/rental/<int:entry_id>", methods=["PUT"])
def api_update_rental(entry_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    entry = get_rental_entry(entry_id)
    if not entry:
        return jsonify({"error": "Entry not found"}), 404
    update_rental_entry(entry_id, data)
    updated = get_rental_entry(entry_id)
    return jsonify({"ok": True, "entry": updated})


@app.route("/api/rental/<int:entry_id>", methods=["DELETE"])
def api_delete_rental(entry_id):
    entry = get_rental_entry(entry_id)
    if not entry:
        return jsonify({"error": "Entry not found"}), 404
    delete_rental_entry(entry_id)
    return jsonify({"ok": True, "deleted": entry_id})


@app.route("/api/rental/week/delete", methods=["POST"])
def api_delete_rental_week():
    data = request.get_json()
    if not data or not data.get("week_start") or not data.get("week_end"):
        return jsonify({"error": "week_start and week_end required"}), 400
    delete_rental_week(data["week_start"], data["week_end"])
    return jsonify({"ok": True})

# ── Sessions API ─────────────────────────────────────────────────────────────
@app.route("/api/sessions")
def api_sessions():
    weeks_back = request.args.get("weeks", 8, type=int)
    if weeks_back > 52:
        weeks_back = 52
    try:
        data = get_sessions_data(weeks_back)
        return jsonify(data)
    except Exception as e:
        logger.exception("Sessions fetch failed")
        return jsonify({"error": str(e)}), 500
# ── FRCF API ─────────────────────────────────────────────────────────────────
@app.route("/api/frcf")
def api_frcf():
    """Fetch First Responder Children's Foundation session data from Google Sheets."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            return jsonify({"error": "Google service account not configured"}), 500

        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)

        FRCF_SHEET_ID = "1TUhgr4l1YfiEjDcSVSlvydfAWx76xstTWZRWzDwKs_k"
        sheet = gc.open_by_key(FRCF_SHEET_ID)
        worksheet = sheet.worksheet("Form Responses 1")
        rows = worksheet.get_all_records()

        sessions = []
        for row in rows:
            sessions.append({
                "timestamp": str(row.get("Timestamp", "")),
                "session_date": str(row.get("Session Date", "")),
                "location": str(row.get("Location", "")),
                "client_id": str(row.get("Client ID #", "")),
                "age": row.get("Age", ""),
                "type_of_service": str(row.get("Type of Service", "")),
                "presenting_problem": str(row.get("Presenting Problem", "")),
                "therapist": str(row.get("Therapist", "")),
            })

        return jsonify({"sessions": sessions, "total": len(sessions)})

    except Exception as e:
        logger.exception("FRCF data fetch failed")
        return jsonify({"error": str(e)}), 500


# ── Startup ──────────────────────────────────────────────────────────────────
port = int(os.environ.get("PORT", 8080))
logger.info("Starting on port %d", port)

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(_do_refresh, "interval", minutes=REFRESH_MINUTES,
                  id="refresh", replace_existing=True)
scheduler.start()

# Pre-load from Redis (non-blocking)
_ensure_loaded()

if __name__ == "__main__":
    _do_refresh()
    app.run(host="0.0.0.0", port=port, debug=False)
