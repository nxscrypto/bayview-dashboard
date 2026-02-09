import requests
import re
from datetime import datetime, timedelta
from collections import defaultdict
import pytz

# Google Calendar public API key (embedded in the public embed page)
GCAL_API_KEY = "AIzaSyBNlYH01_9Hc5S1J9vuFmu2nUqBZJNAXxs"

# Calendar IDs and their location names
CALENDARS = {
      "Coral Springs": "c_mt73384td5orpv4df0rt1al9eo@group.calendar.google.com",
      "Fort Lauderdale": "assistant@bayviewtherapy.com",
      "Plantation": "c_fa9af77a4e259a900ca7500d326c7afd82445e8d3ea2cc5ba6b07a71a22ade45@group.calendar.google.com",
      "Telehealth": "telehealth@bayviewtherapy.com",
}

ET = pytz.timezone("America/New_York")

# Words that are room/type identifiers, not therapist names
ROOM_KEYWORDS = {
      "empower", "dream", "conf", "renew", "inspire", "harmony",
      "serenity", "cs", "ftl", "pl", "maint", "maintenance",
      "conference", "group", "office", "room", "testing",
}

# Known non-session events to skip
SKIP_PATTERNS = [
      r"best air",
      r"maintenance",
      r"maint\b",
      r"lunch",
      r"break",
      r"meeting",
      r"staff",
      r"closed",
      r"holiday",
      r"off\b",
      r"block",
      r"hold",
      r"cancel",
      r"no session",
      r"note\b",
      r"reminder",
      r"admin",
      r"cleaning",
      r"interview",
      r"orientation",
      r"training",
]


def extract_therapist_name(summary):
      """Extract therapist name from calendar event summary.

              Events are formatted like: 'Nicole empower', 'dr brittany conf', 'Tahnee Harmony'
                  The therapist name is typically the first word(s) and the last word is the room/type.
                      """
      if not summary:
                return None

      s = summary.strip()

    # Check if this is a non-session event
      for pattern in SKIP_PATTERNS:
                if re.search(pattern, s, re.IGNORECASE):
                              return None

            parts = s.split()
    if not parts:
              return None

    # Build therapist name from parts that aren't room keywords
    name_parts = []
    for part in parts:
              cleaned = part.strip().lower().rstrip(".,;:")
              if cleaned in ROOM_KEYWORDS:
                            continue
                        name_parts.append(part)

    if not name_parts:
              return None

    # Normalize the name: title case
    name = " ".join(name_parts).strip()
    # Handle 'dr' prefix
    name = re.sub(r"(?i)^dr\.?\s+", "Dr. ", name)
    # Title case each word (except Dr.)
    final_parts = []
    for p in name.split():
              if p.startswith("Dr."):
                            final_parts.append(p)
else:
            final_parts.append(p.capitalize())

    name = " ".join(final_parts)

    if not name or len(name) < 2:
              return None

    return name


def get_week_bounds(date=None):
      """Get Monday-Sunday bounds for the week containing the given date."""
    if date is None:
              date = datetime.now(ET).date()
    # Monday = 0 in weekday()
    monday = date - timedelta(days=date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def get_weeks_in_range(start_date, end_date):
      """Get all week ranges (Mon-Sun) that overlap with the given date range."""
    weeks = []
    current_monday, _ = get_week_bounds(start_date)
    while current_monday <= end_date:
              sunday = current_monday + timedelta(days=6)
        weeks.append((current_monday, sunday))
        current_monday += timedelta(days=7)
    return weeks


def fetch_calendar_events(calendar_id, time_min, time_max):
      """Fetch events from a Google Calendar using the public API."""
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
                      # Skip cancelled events
                      if item.get("status") == "cancelled":
                                        continue

                      summary = item.get("summary", "")
                      start = item.get("start", {})
                      end = item.get("end", {})

            # Get start datetime
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
                    # All-day event - skip these as they're not sessions
                                  continue
except Exception:
                continue

            events.append({
                              "summary": summary,
                              "start": start_dt,
                              "date": start_dt.date(),
                              "day_of_week": start_dt.weekday(),  # 0=Mon, 6=Sun
            })

        page_token = data.get("nextPageToken")
        if not page_token:
                      break

    return events


def get_sessions_data(weeks_back=8):
      """Fetch and process session data from all calendars.

              Returns a structure with sessions grouped by week, location, and therapist.
                  Each therapist row shows Mon-Sun session counts.

                          Args:
                                  weeks_back: Number of weeks of history to fetch (default 8)

                                          Returns:
                                                  dict with keys:
                                                              - weeks: list of week objects with therapist session breakdowns
                                                                          - therapist_totals: summary across all weeks
                                                                                      - locations: list of location names
                                                                                                  - generated: timestamp
                                                                                                      """
    now = datetime.now(ET)
    today = now.date()

    # Calculate date range
    current_monday, current_sunday = get_week_bounds(today)
    start_monday = current_monday - timedelta(weeks=weeks_back - 1)
    # Include next week too for upcoming view
    end_sunday = current_sunday + timedelta(days=7)

    # Make timezone-aware datetime for API
    time_min = ET.localize(datetime.combine(start_monday, datetime.min.time()))
    time_max = ET.localize(datetime.combine(end_sunday, datetime.max.time()))

    # Fetch from all calendars
    all_events = {}  # location -> events list
    for location, cal_id in CALENDARS.items():
              events = fetch_calendar_events(cal_id, time_min, time_max)
        all_events[location] = events

    # Get all weeks in range
    weeks = get_weeks_in_range(start_monday, current_sunday)

    # Process into weekly breakdowns
    weeks_data = []
    therapist_grand_totals = defaultdict(lambda: {"total": 0, "by_location": defaultdict(int)})

    for week_start, week_end in weeks:
              week_label = f"{week_start.strftime('%m/%d')} - {week_end.strftime('%m/%d/%Y')}"
        is_current = week_start == current_monday

        # Collect sessions for this week, by location
        location_data = {}

        for location in CALENDARS:
                      # Filter events for this week
                      week_events = [
                                        e for e in all_events.get(location, [])
                                        if week_start <= e["date"] <= week_end
                      ]

            # Group by therapist
                      therapist_sessions = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0])  # Mon-Sun

            for event in week_events:
                              name = extract_therapist_name(event["summary"])
                              if name:
                                                    dow = event["day_of_week"]  # 0=Mon, 6=Sun
                    therapist_sessions[name][dow] += 1

            # Build rows sorted by therapist name
            rows = []
            for name in sorted(therapist_sessions.keys()):
                              days = therapist_sessions[name]
                              total = sum(days)
                              rows.append({
                                  "therapist": name,
                                  "mon": days[0],
                                  "tue": days[1],
                                  "wed": days[2],
                                  "thu": days[3],
                                  "fri": days[4],
                                  "sat": days[5],
                                  "sun": days[6],
                                  "total": total,
                              })
                              # Update grand totals
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
                      "grand_total": sum(
                                        loc["location_total"] for loc in location_data.values()
                      ),
        })

    # Build therapist summary
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
