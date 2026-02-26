import requests
import re
from datetime import datetime, timedelta
from collections import defaultdict
import pytz

GCAL_API_KEY = "AIzaSyBNlYH01_9Hc5S1J9vuFmu2nUqBZJNAXxs"

CALENDARS = {
    "Coral Springs": "c_mt73384td5orpv4df0rt1al9eo@group.calendar.google.com",
    "Fort Lauderdale": "assistant@bayviewtherapy.com",
    "Plantation": "c_fa9af77a4e259a900ca7500d326c7afd82445e8d3ea2cc5ba6b07a71a22ade45@group.calendar.google.com",
    "Telehealth": "telehealth@bayviewtherapy.com",
}

ET = pytz.timezone("America/New_York")

# All room names used at Bayview locations
ROOM_KEYWORDS = {
    "empower", "dream", "conf", "cof", "renew", "inspire", "harmony",
    "serenity", "tranquil", "hope", "conference",
    "cs", "ftl", "pl", "maint", "maintenance",
    "group", "office", "room", "testing",
}

SKIP_PATTERNS = [
    r"best air", r"maintenance", r"maint\b", r"lunch", r"break",
    r"meeting", r"staff", r"closed", r"holiday", r"off\b",
    r"block", r"hold", r"cancel", r"no session", r"note\b",
    r"reminder", r"admin", r"cleaning", r"interview",
    r"orientation", r"training",
]

# Extra words to strip from names (not therapist names)
STRIP_WORDS = {
    "new", "sean's", "daughter", "son", "wife", "husband",
    "client", "session", "appointment", "eval", "evaluation",
    "intake", "consult", "consultation",
}


def extract_therapist_name(summary):
    """Extract just the therapist first name (+ optional initial) from a calendar summary."""
    if not summary:
        return None
    s = summary.strip()

    # Skip non-session entries
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, s, re.IGNORECASE):
            return None

    # Strip asterisks and trailing punctuation
    s = re.sub(r'\*+', '', s).strip()

    parts = s.split()
    if not parts:
        return None

    name_parts = []
    for part in parts:
        cleaned = part.strip().lower().rstrip(".,;:")
        if cleaned in ROOM_KEYWORDS:
            continue
        if cleaned in STRIP_WORDS:
            continue
        name_parts.append(part)

    if not name_parts:
        return None

    name = " ".join(name_parts).strip()

    # Normalize "Dr." prefix
    name = re.sub(r"(?i)^dr\.?\s+", "Dr. ", name)

    # Capitalize properly
    final_parts = []
    for p in name.split():
        if p.startswith("Dr."):
            final_parts.append(p)
        elif p == "d" or p == "D":
            # Single letter initial like "Heather d" → "Heather D"
            final_parts.append(p.upper())
        elif p == "g" or p == "G":
            final_parts.append(p.upper())
        elif p == "s" or p == "S":
            final_parts.append(p.upper())
        elif "'" in p:
            # Handle names like J'nay
            final_parts.append(p[0].upper() + p[1:])
        else:
            final_parts.append(p.capitalize())
    name = " ".join(final_parts)

    if not name or len(name) < 2:
        return None

    return name


# Canonical name mapping: normalize variants to a single name
# Key = lowercase normalized form, Value = display name
# Built from frequency analysis — most common form wins
CANONICAL_NAMES = {}


def _normalize_key(name):
    """Create a lookup key for deduplication."""
    return re.sub(r'\s+', ' ', name.strip().lower())


def resolve_name(name, name_counts):
    """
    Resolve a therapist name to its canonical form.
    Uses the most frequent variant as the canonical name.
    Merges names that share the same first name (and optional initial).
    """
    key = _normalize_key(name)
    if key in CANONICAL_NAMES:
        return CANONICAL_NAMES[key]
    return name


def build_canonical_map(all_names):
    """
    Given a dict of {name: count}, build canonical name mappings.
    Group by first name (+ optional single-letter initial).
    The most frequent variant becomes canonical.
    """
    # Group names by their "base" (first word, lowercased)
    groups = defaultdict(list)
    for name, count in all_names.items():
        key = _normalize_key(name)
        parts = key.split()
        if not parts:
            continue

        # Base key: first name + optional single-char initial
        base = parts[0]
        if len(parts) > 1 and len(parts[1]) == 1:
            base = f"{parts[0]} {parts[1]}"

        # Special: "dr." prefix → "dr. firstname"
        if base == "dr." and len(parts) > 1:
            base = f"dr. {parts[1]}"
            if len(parts) > 2 and len(parts[2]) == 1:
                base = f"dr. {parts[1]} {parts[2]}"

        groups[base].append((name, count))

    # For each group, pick the most frequent as canonical
    for base, variants in groups.items():
        # Sort by count desc, then by shortest name (prefer clean names)
        variants.sort(key=lambda x: (-x[1], len(x[0])))
        canonical = variants[0][0]
        for name, _ in variants:
            CANONICAL_NAMES[_normalize_key(name)] = canonical


def merge_therapist_sessions(therapist_sessions):
    """
    Merge sessions for therapists whose names resolve to the same canonical form.
    Returns a new dict with canonical names as keys.
    """
    # First build frequency map from current data
    name_counts = {}
    for name, days in therapist_sessions.items():
        name_counts[name] = name_counts.get(name, 0) + sum(days)

    # Build/update canonical map
    build_canonical_map(name_counts)

    # Merge sessions under canonical names
    merged = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    for name, days in therapist_sessions.items():
        canonical = resolve_name(name, name_counts)
        for i in range(7):
            merged[canonical][i] += days[i]

    return merged


def get_week_bounds(date=None):
    if date is None:
        date = datetime.now(ET).date()
    monday = date - timedelta(days=date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def get_weeks_in_range(start_date, end_date):
    weeks = []
    current_monday, _ = get_week_bounds(start_date)
    while current_monday <= end_date:
        sunday = current_monday + timedelta(days=6)
        weeks.append((current_monday, sunday))
        current_monday += timedelta(days=7)
    return weeks


def fetch_calendar_events(calendar_id, time_min, time_max):
    url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
    events = []
    page_token = None
    while True:
        params = {
            "key": GCAL_API_KEY,
            "timeMin": time_min.isoformat(),
            "timeMax": time_max.isoformat(),
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": 2500,
        }
        if page_token:
            params["pageToken"] = page_token
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Error fetching calendar {calendar_id}: {e}")
            break
        for item in data.get("items", []):
            if item.get("status") == "cancelled":
                continue
            summary = item.get("summary", "")
            start = item.get("start", {})
            start_dt_str = start.get("dateTime") or start.get("date")
            if not start_dt_str:
                continue
            try:
                if "T" in start_dt_str:
                    start_dt = datetime.fromisoformat(start_dt_str)
                    if start_dt.tzinfo is None:
                        start_dt = ET.localize(start_dt)
                    else:
                        start_dt = start_dt.astimezone(ET)
                else:
                    continue
            except Exception:
                continue
            # Parse end time for duration calculation
            end_info = item.get("end", {})
            end_dt_str = end_info.get("dateTime") or end_info.get("date")
            end_dt = None
            if end_dt_str and "T" in end_dt_str:
                try:
                    end_dt = datetime.fromisoformat(end_dt_str)
                    if end_dt.tzinfo is None:
                        end_dt = ET.localize(end_dt)
                    else:
                        end_dt = end_dt.astimezone(ET)
                except Exception:
                    end_dt = None

            # Calculate session count based on duration
            # 30 min = 0.5, 60 min = 1.0, 90 min = 1.5, etc.
            if end_dt:
                duration_minutes = (end_dt - start_dt).total_seconds() / 60
                session_count = duration_minutes / 60.0
            else:
                session_count = 1.0

            events.append({
                "summary": summary,
                "start": start_dt,
                "end": end_dt,
                "date": start_dt.date(),
                "day_of_week": start_dt.weekday(),
                "session_count": session_count,
            })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return events


def get_sessions_data(weeks_back=8):
    now = datetime.now(ET)
    today = now.date()
    current_monday, current_sunday = get_week_bounds(today)
    start_monday = current_monday - timedelta(weeks=weeks_back - 1)
    end_sunday = current_sunday + timedelta(days=7)
    time_min = ET.localize(datetime.combine(start_monday, datetime.min.time()))
    time_max = ET.localize(datetime.combine(end_sunday, datetime.max.time()))
    all_events = {}
    for location, cal_id in CALENDARS.items():
        events = fetch_calendar_events(cal_id, time_min, time_max)
        all_events[location] = events

    # First pass: collect ALL names across all weeks/locations for frequency analysis
    global_name_counts = defaultdict(int)
    for location in CALENDARS:
        for event in all_events.get(location, []):
            name = extract_therapist_name(event["summary"])
            if name:
                global_name_counts[name] += 1

    # Build canonical name map from global frequencies
    build_canonical_map(global_name_counts)

    weeks = get_weeks_in_range(start_monday, current_sunday)
    weeks_data = []
    therapist_grand_totals = defaultdict(lambda: {"total": 0, "by_location": defaultdict(int)})
    for week_start, week_end in weeks:
        week_label = f"{week_start.strftime('%m/%d')} - {week_end.strftime('%m/%d/%Y')}"
        is_current = week_start == current_monday
        location_data = {}
        for location in CALENDARS:
            week_events = [
                e for e in all_events.get(location, [])
                if week_start <= e["date"] <= week_end
            ]
            therapist_sessions = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            for event in week_events:
                name = extract_therapist_name(event["summary"])
                if name:
                    # Resolve to canonical name immediately
                    canonical = resolve_name(name, global_name_counts)
                    dow = event["day_of_week"]
                    therapist_sessions[canonical][dow] += event.get("session_count", 1.0)

            rows = []
            for name in sorted(therapist_sessions.keys()):
                days = therapist_sessions[name]
                total = sum(days)
                rows.append({
                    "therapist": name,
                    "mon": days[0], "tue": days[1], "wed": days[2],
                    "thu": days[3], "fri": days[4], "sat": days[5],
                    "sun": days[6], "total": total,
                })
                therapist_grand_totals[name]["total"] += total
                therapist_grand_totals[name]["by_location"][location] += total
            if rows:
                location_data[location] = {
                    "rows": rows,
                    "location_total": sum(r["total"] for r in rows),
                }
        weeks_data.append({
            "week_label": week_label,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "is_current": is_current,
            "locations": location_data,
            "grand_total": sum(loc["location_total"] for loc in location_data.values()),
        })
    therapist_summary = []
    for name in sorted(therapist_grand_totals.keys()):
        info = therapist_grand_totals[name]
        therapist_summary.append({
            "therapist": name,
            "total_sessions": info["total"],
            "by_location": dict(info["by_location"]),
        })
    return {
        "weeks": weeks_data,
        "therapist_summary": therapist_summary,
        "locations": list(CALENDARS.keys()),
        "weeks_back": weeks_back,
        "generated": now.isoformat(),
    }
