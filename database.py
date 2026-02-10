"""
database.py — SQLite database for Bayview lead tracking.
Stores leads submitted via the custom intake form.
"""

import sqlite3
import os
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "/data/bayview.db")


def _ensure_dir():
    d = os.path.dirname(DB_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


@contextmanager
def get_db():
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    _ensure_dir()
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                location TEXT NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT DEFAULT '',
                service_type TEXT NOT NULL,
                presenting_problem TEXT NOT NULL,
                referral_source TEXT NOT NULL,
                action_taken TEXT NOT NULL,
                referred_to TEXT NOT NULL,
                marketing_program TEXT NOT NULL DEFAULT 'No',
                referral_outcome TEXT NOT NULL,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        logger.info("Database initialized at %s", DB_PATH)


def add_lead(data):
    now = datetime.now().isoformat()
    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO leads (date, location, first_name, last_name, phone, email,
                service_type, presenting_problem, referral_source, action_taken,
                referred_to, marketing_program, referral_outcome, notes,
                created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("date", ""),
            data.get("location", ""),
            data.get("first_name", ""),
            data.get("last_name", ""),
            data.get("phone", ""),
            data.get("email", ""),
            data.get("service_type", ""),
            data.get("presenting_problem", ""),
            data.get("referral_source", ""),
            data.get("action_taken", ""),
            data.get("referred_to", ""),
            data.get("marketing_program", "No"),
            data.get("referral_outcome", ""),
            data.get("notes", ""),
            now, now
        ))
        return cur.lastrowid


def update_lead(lead_id, data):
    now = datetime.now().isoformat()
    fields = []
    values = []
    allowed = ["referred_to", "referral_outcome", "action_taken",
               "marketing_program", "notes", "location", "service_type",
               "presenting_problem", "referral_source", "phone", "email"]
    for key in allowed:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])
    if not fields:
        return False
    fields.append("updated_at = ?")
    values.append(now)
    values.append(lead_id)
    with get_db() as conn:
        conn.execute(f"UPDATE leads SET {', '.join(fields)} WHERE id = ?", values)
    return True


def get_pending_leads(days=14):
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM leads
            WHERE (referred_to = 'Pending' OR action_taken = 'Pending' OR referral_outcome IN ('Called', 'Emailed', 'Left Message', 'Pending'))
              AND date >= ?
            ORDER BY date DESC, created_at DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def get_recent_leads(days=30):
    """Return all leads from the last N days."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM leads
            WHERE date >= ?
            ORDER BY date DESC, created_at DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def get_all_leads():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM leads ORDER BY date DESC, created_at DESC").fetchall()
    return [dict(r) for r in rows]


def get_lead(lead_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    return dict(row) if row else None


def delete_lead(lead_id):
    with get_db() as conn:
        conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    return True


def get_leads_for_dashboard():
    """Return leads in a format compatible with the Google Sheet CSV structure."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT date, first_name, last_name, phone, email, service_type,
                   presenting_problem, referral_source, action_taken, referred_to,
                   referral_outcome, notes, marketing_program, location, created_at
            FROM leads ORDER BY date ASC
        """).fetchall()
    return [dict(r) for r in rows]



# ── Room Rental Entries ─────────────────────────────────────────────────────

def init_rental_db():
    _ensure_dir()
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rental_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start TEXT NOT NULL,
                week_end TEXT NOT NULL,
                therapist TEXT NOT NULL,
                location TEXT NOT NULL DEFAULT '',
                amount REAL NOT NULL DEFAULT 0,
                category TEXT NOT NULL DEFAULT 'room_rental',
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        # Index for fast lookups by week
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rental_week
            ON rental_entries(week_start, week_end)
        """)
        logger.info("Rental entries table initialized")


def add_rental_entry(data):
    now = datetime.now().isoformat()
    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO rental_entries
            (week_start, week_end, therapist, location, amount, category, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("week_start", ""),
            data.get("week_end", ""),
            data.get("therapist", ""),
            data.get("location", ""),
            float(data.get("amount", 0)),
            data.get("category", "room_rental"),
            data.get("notes", ""),
            now, now
        ))
        return cur.lastrowid


def add_rental_entries_bulk(entries):
    """Insert multiple rental entries at once (for a whole week)."""
    now = datetime.now().isoformat()
    ids = []
    with get_db() as conn:
        for data in entries:
            cur = conn.execute("""
                INSERT INTO rental_entries
                (week_start, week_end, therapist, location, amount, category, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get("week_start", ""),
                data.get("week_end", ""),
                data.get("therapist", ""),
                data.get("location", ""),
                float(data.get("amount", 0)),
                data.get("category", "room_rental"),
                data.get("notes", ""),
                now, now
            ))
            ids.append(cur.lastrowid)
    return ids


def update_rental_entry(entry_id, data):
    now = datetime.now().isoformat()
    fields = []
    values = []
    allowed = ["therapist", "location", "amount", "category", "notes", "week_start", "week_end"]
    for key in allowed:
        if key in data:
            val = float(data[key]) if key == "amount" else data[key]
            fields.append(f"{key} = ?")
            values.append(val)
    if not fields:
        return False
    fields.append("updated_at = ?")
    values.append(now)
    values.append(entry_id)
    with get_db() as conn:
        conn.execute(f"UPDATE rental_entries SET {', '.join(fields)} WHERE id = ?", values)
    return True


def delete_rental_entry(entry_id):
    with get_db() as conn:
        conn.execute("DELETE FROM rental_entries WHERE id = ?", (entry_id,))
    return True


def get_rental_entry(entry_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM rental_entries WHERE id = ?", (entry_id,)).fetchone()
        return dict(row) if row else None


def get_rental_entries_by_week(week_start, week_end=None):
    """Get all rental entries for a specific week."""
    with get_db() as conn:
        if week_end:
            rows = conn.execute(
                "SELECT * FROM rental_entries WHERE week_start = ? AND week_end = ? ORDER BY therapist",
                (week_start, week_end)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM rental_entries WHERE week_start = ? ORDER BY therapist",
                (week_start,)
            ).fetchall()
        return [dict(r) for r in rows]


def get_recent_rental_entries(weeks=12):
    """Get rental entries from the last N weeks."""
    cutoff = (datetime.now() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM rental_entries WHERE week_start >= ? ORDER BY week_start DESC, therapist",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_rental_weeks():
    """Get distinct weeks that have rental entries."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT week_start, week_end FROM rental_entries ORDER BY week_start DESC"
        ).fetchall()
        return [{"week_start": r["week_start"], "week_end": r["week_end"]} for r in rows]


def get_all_rental_entries():
    """Get ALL rental entries for dashboard merging."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM rental_entries ORDER BY week_start ASC, therapist"
        ).fetchall()
    return [dict(r) for r in rows]

def delete_rental_week(week_start, week_end):
    """Delete all entries for a specific week."""
    with get_db() as conn:
        conn.execute(
            "DELETE FROM rental_entries WHERE week_start = ? AND week_end = ?",
            (week_start, week_end)
        )
    return True
