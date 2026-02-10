"""
data_processor.py — Fetches live CSV data from Google Sheets and computes
all dashboard metrics for the Bayview Lead Dashboard.
"""

import csv
import json
import io
import logging

try:
    from database import get_leads_for_dashboard, get_all_rental_entries
    HAS_DB = True
except ImportError:
    HAS_DB = False
import requests
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

LEAD_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vToEXdlOhqCTqOaPDGSDVDG-quj4I4gjjDJldICIwwsAeNv_dA6QXrCCleYSxQ2_4KkU87RxW8gHZ8M"
    "/pub?gid=1560459172&single=true&output=csv"
)
RENTAL_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRk4SQX3Rlq8wgZTcM6_hOAHkoAGWws54X-N-IwvCspQxDlJ-mNgD_BGDKcPIgrwnwUB9a-6Af1gosV"
    "/pub?gid=1158820692&single=true&output=csv"
)

ROOM_RENTAL = 40
AVG_SESSIONS = 3
THERAPY_REV = ROOM_RENTAL * AVG_SESSIONS
TESTING_REV = 1500


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def fetch_csv(url: str) -> list[list[str]]:
    logger.info("Fetching %s …", url[:80])
    resp = requests.get(url, timeout=30, headers={"User-Agent": "BayviewDashboard/1.0"})
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)
    logger.info("  → %d rows", len(rows))
    return rows


def parse_date(s: str):
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).date()
            if 2017 <= dt.year <= date.today().year + 1:
                return dt
            return None
        except ValueError:
            continue
    return None


def parse_dollar(s: str) -> float:
    if not s or not s.strip():
        return 0.0
    s = s.strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Normalizers
# ──────────────────────────────────────────────────────────────────────────────

def normalize_location(loc: str) -> str:
    if not loc:
        return "Unknown"
    loc = loc.strip()
    lo = loc.lower()
    if "fort" in lo or "ftl" in lo or "lauderdale" in lo:
        return "Fort Lauderdale"
    if "coral" in lo or lo == "cs":
        return "Coral Springs"
    if "plantation" in lo or lo == "pl":
        return "Plantation"
    if "tele" in lo:
        return "Telehealth"
    return loc if loc else "Unknown"


def normalize_service(svc: str):
    if not svc:
        return None
    lo = svc.strip().lower()
    if "individual" in lo:
        return "Individual Therapy"
    if "couple" in lo:
        return "Couples Therapy"
    if "adolescent" in lo or "teen" in lo:
        return "Adolescent Therapy"
    if "child" in lo and "adolescent" not in lo:
        return "Child Therapy"
    if "psych" in lo and "evaluation" in lo:
        return "Psychological Evaluation"
    if "testing" in lo or "evaluation" in lo:
        return "Testing Evaluation"
    if "psychiat" in lo:
        return "Psychiatric"
    if "family" in lo:
        return "Family Therapy"
    if "group" in lo:
        return "Group Therapy"
    if "cog" in lo:
        return "CogScreen"
    if "superv" in lo:
        return "Supervision"
    return svc.strip()


def is_testing_service(svc: str) -> bool:
    if not svc:
        return False
    lo = svc.lower()
    return "testing" in lo or "evaluation" in lo or "cogscreen" in lo


def normalize_source(src: str) -> str:
    if not src:
        return "Unknown"
    src = src.strip()
    lo = src.lower()
    if "google" in lo:
        return "Google"
    if "doctor" in lo or "physician" in lo or "pediatrician" in lo:
        return "Doctors"
    if "psychology today" in lo:
        return "Psychology Today"
    if "previous client" in lo:
        return "Previous Clients"
    if "frcf" in lo:
        return "FRCF"
    if "alma" in lo:
        return "ALMA"
    if "bayview therapist" in lo:
        return "Bayview Therapists"
    if "family" in lo and "friend" in lo:
        return "Family/Friends"
    if "colleague" in lo:
        return "Colleagues"
    if "yelp" in lo:
        return "Yelp"
    if "social media" in lo or "instagram" in lo or "facebook" in lo:
        return "Social Media"
    if "bts" in lo:
        return "BTS Therapists"
    if "school" in lo:
        return "Schools"
    if "bsac" in lo:
        return "BSAC"
    if "psychiatrist" in lo:
        return "Psychiatrists"
    return src


def normalize_outcome(outcome: str) -> str:
    if not outcome:
        return "Unknown"
    outcome = outcome.strip()
    lo = outcome.lower()
    if lo in ("booked", "boked"):
        return "Booked"
    if any(k in lo for k in ("no response", "never booked", "no answer", "did not book",
                              "not interested", "looking for", "insurance", "wrong number", "voicemail")):
        return "Never Booked"
    if lo.startswith("called") or ("called" in lo and len(outcome) < 20):
        return "Called"
    if "cancel" in lo:
        return "Cancelled"
    if "email" in lo:
        return "Emailed"
    if "pending" in lo or "waiting" in lo:
        return "Pending"
    if "left message" in lo or "left msg" in lo:
        return "Left Message"
    return outcome


def normalize_team_member(name: str):
    if not name:
        return None
    name = name.strip()
    if name.lower() in ("no response", "no", "yes", "", "n/a", "none", "x", "no answer"):
        return None
    return name


def get_action(action: str):
    if not action:
        return None
    lo = action.strip().lower()
    if "bayview" in lo and "therapist" in lo:
        return "Referred to Bayview Therapist"
    if "outside" in lo or "referred out" in lo:
        return "Referred to Outside Provider"
    if "scheduled" in lo:
        return "Scheduled Appointment"
    if "bts" in lo:
        return "Referred to BTS Therapist"
    if "pending" in lo:
        return "Pending"
    return action.strip()


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation helpers
# ──────────────────────────────────────────────────────────────────────────────

def count_top(items, n=20):
    return [{"name": k, "count": v} for k, v in Counter(items).most_common(n)]


def count_top_locs(items, n=5):
    loc_leads, loc_booked = Counter(), Counter()
    for loc, booked in items:
        loc_leads[loc] += 1
        if booked:
            loc_booked[loc] += 1
    return [{"name": l, "leads": c, "booked": loc_booked.get(l, 0)} for l, c in loc_leads.most_common(n)]


# ──────────────────────────────────────────────────────────────────────────────
# Build a period slice (all / ytd / lastyear / month / week / today)
# ──────────────────────────────────────────────────────────────────────────────

def build_period(leads, prev_leads=None):
    total = len(leads)
    booked = sum(1 for l in leads if l["booked"])
    booking_rate = round(booked / total * 100) if total else 0

    loc_c = Counter(l["location"] for l in leads)
    src_c = Counter(l["source"] for l in leads)
    svc_c = Counter(l["service"] for l in leads if l["service"])
    top_loc = loc_c.most_common(1)[0] if loc_c else ("Unknown", 0)
    top_src = src_c.most_common(1)[0] if src_c else ("Unknown", 0)
    top_svc = svc_c.most_common(1)[0] if svc_c else ("Unknown", 0)

    # Daily
    daily_m = defaultdict(lambda: {"leads": 0, "booked": 0})
    for l in leads:
        d = l["date"].isoformat()
        daily_m[d]["leads"] += 1
        if l["booked"]:
            daily_m[d]["booked"] += 1
    daily = [{"date": k, **v} for k, v in sorted(daily_m.items())]

    # Monthly
    mon_m = defaultdict(lambda: {"leads": 0, "booked": 0})
    for l in leads:
        m = l["date"].strftime("%Y-%m")
        mon_m[m]["leads"] += 1
        if l["booked"]:
            mon_m[m]["booked"] += 1
    monthly = [{"month": k, **v} for k, v in sorted(mon_m.items())]

    # Yearly
    yr_m = defaultdict(lambda: {"leads": 0, "booked": 0})
    for l in leads:
        y = str(l["date"].year)
        yr_m[y]["leads"] += 1
        if l["booked"]:
            yr_m[y]["booked"] += 1
    yearly = [{"year": k, **v} for k, v in sorted(yr_m.items())]

    locations = count_top_locs([(l["location"], l["booked"]) for l in leads])
    services = count_top([l["service"] for l in leads if l["service"]], 15)
    problems = count_top([l["problem"] for l in leads if l["problem"]], 15)
    sources = count_top([l["source"] for l in leads], 20)
    outcomes = count_top([l["outcome"] for l in leads], 40)
    actions = count_top([l["action"] for l in leads if l["action"]], 10)
    marketing = count_top([l["marketing"] for l in leads if l["marketing"]])

    # Team
    tm = defaultdict(lambda: {"leads": 0, "booked": 0, "mkt": False})
    for l in leads:
        if l["team_member"]:
            tm[l["team_member"]]["leads"] += 1
            if l["booked"]:
                tm[l["team_member"]]["booked"] += 1
            if l["marketing"] == "Yes":
                tm[l["team_member"]]["mkt"] = True
    NON_TEAM_NAMES = {"insurance", "medicaid", "pending", "medicare"}
    all_team = sorted(
        [{"name": n, "leads": d["leads"], "booked": d["booked"],
          "rate": round(d["booked"] / d["leads"] * 100) if d["leads"] else 0,
          "mkt": d["mkt"]} for n, d in tm.items() if d["leads"] >= 1],
        key=lambda x: -x["leads"]
    )
    team = [t for t in all_team if t["name"].lower() not in NON_TEAM_NAMES][:40]
    referrals = [t for t in all_team if t["name"].lower() in {"insurance", "medicaid", "medicare"}]
    pending = [t for t in all_team if t["name"].lower() == "pending"]

    # Revenue
    therapy_booked = sum(1 for l in leads if l["booked"] and not is_testing_service(l.get("service_raw", "")))
    testing_booked = sum(1 for l in leads if l["booked"] and is_testing_service(l.get("service_raw", "")))
    therapy_total = sum(1 for l in leads if not is_testing_service(l.get("service_raw", "")))
    testing_total = sum(1 for l in leads if is_testing_service(l.get("service_raw", "")))

    result = {
        "total": total, "booked": booked, "bookingRate": booking_rate,
        "topLocation": {"name": top_loc[0], "count": top_loc[1]},
        "topSource": {"name": top_src[0], "count": top_src[1]},
        "topService": {"name": top_svc[0], "count": top_svc[1]},
        "daily": daily, "monthly": monthly, "yearly": yearly,
        "locations": locations, "services": services, "problems": problems,
        "sources": sources, "outcomes": outcomes, "actions": actions,
        "marketing": marketing, "team": team,
        "referrals": referrals,
        "pending": pending,
        "revenue": {"therapyBooked": therapy_booked, "testingBooked": testing_booked,
                     "therapyTotal": therapy_total, "testingTotal": testing_total},
    }
    if prev_leads is not None:
        pt = len(prev_leads)
        pb = sum(1 for l in prev_leads if l["booked"])
        result["prev"] = {"total": pt, "booked": pb,
                          "bookingRate": round(pb / pt * 100) if pt else 0}
    return result




# ──────────────────────────────────────────────────────────────────────────────
# Convert custom DB leads to dashboard format
# ──────────────────────────────────────────────────────────────────────────────

def convert_db_leads():
    """Fetch leads from SQLite DB and convert to same format as CSV-parsed leads."""
    if not HAS_DB:
        return []
    try:
        db_rows = get_leads_for_dashboard()
    except Exception as e:
        logger.warning("Could not load DB leads: %s", e)
        return []

    leads = []
    for row in db_rows:
        dt = parse_date(row.get("date", ""))
        if not dt:
            continue
        svc_raw = row.get("service_type", "")
        out_raw = row.get("referral_outcome", "")
        booked = out_raw.lower() in ("booked", "boked") if out_raw else False
        leads.append({
            "date": dt,
            "service": normalize_service(svc_raw) if svc_raw else None,
            "service_raw": svc_raw,
            "problem": row.get("presenting_problem") or None,
            "source": normalize_source(row.get("referral_source", "")) if row.get("referral_source") else "Unknown",
            "action": get_action(row.get("action_taken", "")) if row.get("action_taken") else None,
            "team_member": normalize_team_member(row.get("referred_to", "")) if row.get("referred_to") else None,
            "outcome": normalize_outcome(out_raw) if out_raw else "Unknown",
            "booked": booked,
            "marketing": row.get("marketing_program") or None,
            "location": normalize_location(row.get("location", "")) if row.get("location") else "Unknown",
        })
    logger.info("Loaded %d leads from custom DB", len(leads))
    return leads

# ──────────────────────────────────────────────────────────────────────────────
# Process lead CSV rows
# ──────────────────────────────────────────────────────────────────────────────

def process_leads(rows):
    leads = []
    for row in rows[1:]:
        if len(row) < 11:
            continue
        dt = parse_date(row[1])
        if not dt:
            continue
        svc_raw = row[6].strip() if len(row) > 6 else ""
        out_raw = row[11].strip() if len(row) > 11 else ""
        booked = out_raw.lower() in ("booked", "boked") if out_raw else False
        leads.append({
            "date": dt,
            "service": normalize_service(svc_raw) if svc_raw else None,
            "service_raw": svc_raw,
            "problem": row[7].strip() if len(row) > 7 and row[7].strip() else None,
            "source": normalize_source(row[8]) if len(row) > 8 else "Unknown",
            "action": get_action(row[9]) if len(row) > 9 else None,
            "team_member": normalize_team_member(row[10]) if len(row) > 10 else None,
            "outcome": normalize_outcome(out_raw) if out_raw else "Unknown",
            "booked": booked,
            "marketing": row[13].strip() if len(row) > 13 and row[13].strip() else None,
            "location": normalize_location(row[14]) if len(row) > 14 else "Unknown",
        })
    return leads


# ──────────────────────────────────────────────────────────────────────────────
# Process rental CSV rows
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Convert DB rental entries to weekly format for merging
# ──────────────────────────────────────────────────────────────────────────────
def convert_db_rental():
    """Fetch rental entries from SQLite DB and convert to weekly + therapist format."""
    if not HAS_DB:
        return [], []
    try:
        db_rows = get_all_rental_entries()
    except Exception as e:
        logger.warning("Could not load DB rental entries: %s", e)
        return [], []

    # Group by week
    week_map = defaultdict(lambda: {"total": 0, "cs": 0, "ftl": 0, "pl": 0, "mkt": 0, "testing": 0})
    therapist_totals = defaultdict(float)

    loc_map = {"FTL": "ftl", "CS": "cs", "PL": "pl"}

    for row in db_rows:
        ws = parse_date(row.get("week_start", ""))
        we = parse_date(row.get("week_end", ""))
        if not ws:
            continue
        if not we:
            we = ws

        amt = float(row.get("amount", 0))
        if amt <= 0:
            continue

        loc = row.get("location", "FTL")
        cat = row.get("category", "room_rental")
        therapist = row.get("therapist", "")
        week_key = ws.isoformat()

        # Add to week totals
        week_map[week_key]["total"] += amt
        if cat == "marketing":
            week_map[week_key]["mkt"] += amt
            t_loc = "MKT"
        elif cat == "testing":
            week_map[week_key]["testing"] += amt
            t_loc = "Testing"
        else:
            loc_key = loc_map.get(loc, "ftl")
            week_map[week_key][loc_key] += amt
            t_loc = loc

        # Track per-therapist totals
        if therapist:
            therapist_totals[(therapist, therapist, t_loc)] += amt

        # Store dates for later
        if "start_date" not in week_map[week_key] or ws < week_map[week_key].get("_start_date", ws):
            week_map[week_key]["_start_date"] = ws
        if "end_date" not in week_map[week_key] or we > week_map[week_key].get("_end_date", we):
            week_map[week_key]["_end_date"] = we

    weekly = []
    for week_key, wdata in sorted(week_map.items()):
        sd = wdata.pop("_start_date", parse_date(week_key))
        ed = wdata.pop("_end_date", sd)
        weekly.append({
            "week": week_key,
            "total": int(wdata["total"]),
            "cs": int(wdata["cs"]),
            "ftl": int(wdata["ftl"]),
            "pl": int(wdata["pl"]),
            "mkt": int(wdata["mkt"]),
            "testing": int(wdata["testing"]),
            "start_date": sd,
            "end_date": ed,
        })

    therapists = [{"name": k[0], "col": k[1], "loc": k[2], "total": int(v)} for k, v in therapist_totals.items()]

    logger.info("Loaded %d rental weeks, %d therapist entries from DB", len(weekly), len(therapists))
    return weekly, therapists


def process_rental(rows):
    headers = rows[0]
    therapist_cols = []
    summary_cols = {}

    for i, h in enumerate(headers):
        h = h.strip()
        if not h:
            continue
        if h == "Grand Total":
            summary_cols["gt"] = i
        elif h == "Marketing Total":
            summary_cols["mkt"] = i
        elif h == "Testing":
            summary_cols["testing"] = i
        elif h == "Coral Springs":
            summary_cols["cs"] = i
        elif h == "Fort Lauderdale":
            summary_cols["ftl"] = i
        elif h == "Plantation":
            summary_cols["pl"] = i
        elif i >= 2:
            # Skip summary columns
            if h in ("Grand Total", "Marketing Total", "Testing", "Coral Springs", "Fort Lauderdale", "Plantation"):
                continue
            # Skip supervisor/fixed/open/empty columns
            if "- Sup" in h or "- Fixed" in h or h.startswith("Open") or not h.strip():
                continue
            # Determine location and strip prefix
            loc = "FTL"
            name = h
            if h.startswith("CS:") or h.startswith("CS "):
                loc = "CS"
            elif h.startswith("PL:") or h.startswith("PL "):
                loc = "PL"
            elif h.startswith("Testing:") or h.startswith("Testing "):
                loc = "Testing"
            elif h.startswith("M-") or h.startswith("Mark-"):
                loc = "MKT"
            for pfx in ("FTL: ", "CS: ", "PL: ", "FTL:", "CS:", "PL:", "Testing: ", "Testing:", "Mark-", "M-"):
                if h.startswith(pfx):
                    name = h[len(pfx):].strip()
                    break
            # Skip if name is empty or just a number after prefix stripping
            if not name or name in ("3", "4", "5", "6", "7", "8"):
                continue
            therapist_cols.append({"idx": i, "col": h, "name": name, "loc": loc})

    weekly = []
    all_therapist_totals = defaultdict(float)

    for row in rows[1:]:
        if len(row) <= max(summary_cols.values(), default=0):
            continue
        start_date = parse_date(row[0])
        if not start_date:
            continue
        end_date = parse_date(row[1]) if len(row) > 1 else start_date
        if not end_date:
            end_date = start_date
        gt = parse_dollar(row[summary_cols.get("gt", 98)])
        if gt == 0:
            continue
        mkt = parse_dollar(row[summary_cols.get("mkt", 99)])
        testing = parse_dollar(row[summary_cols.get("testing", 100)])
        cs = parse_dollar(row[summary_cols.get("cs", 101)])
        ftl = parse_dollar(row[summary_cols.get("ftl", 102)])
        pl = parse_dollar(row[summary_cols.get("pl", 103)])

        weekly.append({
            "week": start_date.isoformat(),
            "total": int(gt), "cs": int(cs), "ftl": int(ftl), "pl": int(pl),
            "mkt": int(mkt), "testing": int(testing),
            "start_date": start_date,
            "end_date": end_date,
        })
        for tc in therapist_cols:
            idx = tc["idx"]
            if idx < len(row):
                val = parse_dollar(row[idx])
                if val > 0:
                    all_therapist_totals[(tc["name"], tc["col"], tc["loc"])] += val

    weekly.sort(key=lambda w: w["week"])

    # Monthly & yearly
    mon_map = defaultdict(lambda: {"gt": 0, "cs": 0, "ftl": 0, "pl": 0, "mkt": 0, "testing": 0, "weeks": 0})
    yr_map = defaultdict(lambda: {"gt": 0, "cs": 0, "ftl": 0, "pl": 0, "mkt": 0, "testing": 0})
    for w in weekly:
        ed = w.get("end_date", w["start_date"])
        m = ed.strftime("%Y-%m")
        mon_map[m]["gt"] += w["total"]; mon_map[m]["cs"] += w["cs"]
        mon_map[m]["ftl"] += w["ftl"]; mon_map[m]["pl"] += w["pl"]
        mon_map[m]["mkt"] += w["mkt"]; mon_map[m]["testing"] += w["testing"]
        mon_map[m]["weeks"] += 1
        y = str(ed.year)
        yr_map[y]["gt"] += w["total"]; yr_map[y]["cs"] += w["cs"]
        yr_map[y]["ftl"] += w["ftl"]; yr_map[y]["pl"] += w["pl"]
        yr_map[y]["mkt"] += w["mkt"]; yr_map[y]["testing"] += w["testing"]

    rental_monthly = [{"month": k, **v} for k, v in sorted(mon_map.items())]
    rental_yearly = [{"year": k, **v} for k, v in sorted(yr_map.items())]

    all_entries = [{"name": k[0], "col": k[1], "loc": k[2], "total": int(v)} for k, v in all_therapist_totals.items()]
    therapists_all = sorted([e for e in all_entries if e["loc"] not in ("MKT", "Testing")], key=lambda x: -x["total"])[:60]
    mkt_therapists_all = sorted([e for e in all_entries if e["loc"] == "MKT"], key=lambda x: -x["total"])
    test_therapists_all = sorted([e for e in all_entries if e["loc"] == "Testing"], key=lambda x: -x["total"])

    weekly_clean = [{k: v for k, v in w.items() if k not in ("start_date", "end_date")} for w in weekly]
    # Last 52 weeks for chart
    today = date.today()
    cutoff_52 = today - timedelta(weeks=52)
    weekly_52 = [{k: v for k, v in w.items() if k not in ("start_date", "end_date")} for w in weekly if w["start_date"] >= cutoff_52]

    def period_summary(wdata):
        gt = sum(w["total"] for w in wdata)
        cs = sum(w["cs"] for w in wdata)
        ftl = sum(w["ftl"] for w in wdata)
        pl = sum(w["pl"] for w in wdata)
        mkt = sum(w["mkt"] for w in wdata)
        testing = sum(w["testing"] for w in wdata)
        weeks = len(wdata)
        return {"gt": int(gt), "cs": int(cs), "ftl": int(ftl), "pl": int(pl),
                "mkt": int(mkt), "testing": int(testing), "weeks": weeks,
                "avgWeek": int(gt / weeks) if weeks else 0}

    def period_therapists(wdata, n=60, loc_filter=None):
        dates = {w["start_date"] for w in wdata if "start_date" in w}
        totals = defaultdict(float)
        for row in rows[1:]:
            if len(row) <= max(summary_cols.values(), default=0):
                continue
            sd = parse_date(row[0])
            if not sd or sd not in dates:
                continue
            gt = parse_dollar(row[summary_cols.get("gt", 98)])
            if gt == 0:
                continue
            for tc in therapist_cols:
                if loc_filter and tc["loc"] not in loc_filter:
                    continue
                if loc_filter is None and tc["loc"] in ("MKT", "Testing"):
                    continue
                idx = tc["idx"]
                if idx < len(row):
                    val = parse_dollar(row[idx])
                    if val > 0:
                        totals[(tc["name"], tc["col"], tc["loc"])] += val
        return sorted(
            [{"name": k[0], "col": k[1], "loc": k[2], "total": int(v)} for k, v in totals.items()],
            key=lambda x: -x["total"])[:n]

    today = date.today()
    this_year = [w for w in weekly if w.get("end_date", w["start_date"]).year == today.year]
    last_year = [w for w in weekly if w.get("end_date", w["start_date"]).year == today.year - 1]
    this_month = [w for w in weekly if w.get("end_date", w["start_date"]).year == today.year and w.get("end_date", w["start_date"]).month == today.month]

    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    this_week = [w for w in weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= week_end and w["end_date"] >= week_start]
    last_week_start = week_start - timedelta(days=7)
    last_week_end = week_start - timedelta(days=1)
    last_week = [w for w in weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= last_week_end and w["end_date"] >= last_week_start]
    today_data = [w for w in weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= today <= w["end_date"]]

    lm = (date(today.year, today.month, 1) - timedelta(days=1))
    last_month = [w for w in weekly if w.get("end_date", w["start_date"]).year == lm.year and w.get("end_date", w["start_date"]).month == lm.month]
    prev_ytd_end = date(today.year - 1, today.month, today.day)
    prev_ytd = [w for w in weekly if w.get("end_date", w["start_date"]).year == today.year - 1 and w.get("end_date", w["start_date"]) <= prev_ytd_end]
    prev_ly = [w for w in weekly if w.get("end_date", w["start_date"]).year == today.year - 2]

    return {
        "weekly": weekly_clean, "weekly52": weekly_52, "monthly": rental_monthly, "yearly": rental_yearly,
        "therapists": therapists_all,
        "mktTherapists": mkt_therapists_all,
        "testTherapists": test_therapists_all,
        "allTime": period_summary(weekly),
        "ytd": period_summary(this_year),
        "lastYear": period_summary(last_year),
        "thisMonth": period_summary(this_month),
        "thisWeek": period_summary(this_week),
        "lastWeek": period_summary(last_week),
        "today": period_summary(today_data),
        "lastMonth": period_summary(last_month),
        "prevYtd": period_summary(prev_ytd),
        "prevLy": period_summary(prev_ly),
        "ytdTherapists": period_therapists(this_year),
        "lyTherapists": period_therapists(last_year),
        "thisMonthTherapists": period_therapists(this_month),
        "thisWeekTherapists": period_therapists(this_week),
        "lastWeekTherapists": period_therapists(last_week),
        "todayTherapists": period_therapists(today_data),
        "lastMonthTherapists": period_therapists(last_month),
        "mktYtdTherapists": period_therapists(this_year, loc_filter=["MKT"]),
        "mktLyTherapists": period_therapists(last_year, loc_filter=["MKT"]),
        "mktThisMonthTherapists": period_therapists(this_month, loc_filter=["MKT"]),
        "mktThisWeekTherapists": period_therapists(this_week, loc_filter=["MKT"]),
        "mktLastWeekTherapists": period_therapists(last_week, loc_filter=["MKT"]),
        "testYtdTherapists": period_therapists(this_year, loc_filter=["Testing"]),
        "testLyTherapists": period_therapists(last_year, loc_filter=["Testing"]),
        "testThisMonthTherapists": period_therapists(this_month, loc_filter=["Testing"]),
        "testThisWeekTherapists": period_therapists(this_week, loc_filter=["Testing"]),
        "testLastWeekTherapists": period_therapists(last_week, loc_filter=["Testing"]),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Monthly revenue & cashflow
# ──────────────────────────────────────────────────────────────────────────────

def build_monthly_revenue(leads):
    mon = defaultdict(lambda: {"therapyBooked": 0, "testingBooked": 0})
    for l in leads:
        if not l["booked"]:
            continue
        m = l["date"].strftime("%Y-%m")
        if is_testing_service(l.get("service_raw", "")):
            mon[m]["testingBooked"] += 1
        else:
            mon[m]["therapyBooked"] += 1
    return [{"month": k, **v} for k, v in sorted(mon.items())]


def build_cashflow(leads, rental_weekly):
    today = date.today()
    recent = [l for l in leads if l["date"] >= today - timedelta(days=90)]
    weeks_span = max(1, 90 / 7)
    therapy_pw = round(sum(1 for l in recent if l["booked"] and not is_testing_service(l.get("service_raw", ""))) / weeks_span, 1)
    testing_pw = round(sum(1 for l in recent if l["booked"] and is_testing_service(l.get("service_raw", ""))) / weeks_span, 1)

    today_week_start = today - timedelta(days=today.weekday())
    start_cf = today - timedelta(weeks=14)
    start_cf_monday = start_cf - timedelta(days=start_cf.weekday())

    cf_weekly = []
    for i in range(30):
        ws = start_cf_monday + timedelta(weeks=i)
        we = ws + timedelta(days=6)
        is_past = we < today

        wl = [l for l in leads if ws <= l["date"] <= we]
        tn = sum(1 for l in wl if l["booked"] and not is_testing_service(l.get("service_raw", "")))
        xn = sum(1 for l in wl if l["booked"] and is_testing_service(l.get("service_raw", "")))

        if is_past or ws <= today <= we:
            proj = False
        else:
            tn, xn = round(therapy_pw), round(testing_pw)
            proj = True

        lt, mt, ht = int(tn * ROOM_RENTAL * 1), int(tn * ROOM_RENTAL * 2), int(tn * ROOM_RENTAL * 4)
        lx = mx = hx = int(xn * TESTING_REV)

        cf_weekly.append({
            "week": ws.isoformat(), "isPast": is_past,
            "lowT": lt, "lowX": lx, "low": lt + lx, "lowNc": tn, "lowNx": xn,
            "medT": mt, "medX": mx, "med": mt + mx, "medNc": tn, "medNx": xn,
            "highT": ht, "highX": hx, "high": ht + hx, "highNc": tn, "highNx": xn,
            "proj": proj,
        })

    mon_map = defaultdict(lambda: {"isPast": True, "lowT": 0, "lowX": 0, "low": 0,
                                     "medT": 0, "medX": 0, "med": 0,
                                     "highT": 0, "highX": 0, "high": 0})
    for w in cf_weekly:
        wd = parse_date(w["week"])
        if not wd:
            continue
        m = wd.strftime("%Y-%m")
        for k in ("lowT", "lowX", "low", "medT", "medX", "med", "highT", "highX", "high"):
            mon_map[m][k] += w[k]
        if w["proj"]:
            mon_map[m]["isPast"] = False

    return {
        "weekly": cf_weekly,
        "monthly": [{"month": k, **v} for k, v in sorted(mon_map.items())],
        "rates": {"therapyPerWeek": therapy_pw, "testingPerWeek": testing_pw},
        "todayWeek": today_week_start.isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main entry point — returns the full PRECOMPUTED dict
# ──────────────────────────────────────────────────────────────────────────────

def merge_rental_data(rental, db_weekly, db_therapists):
    """Merge DB rental weekly/therapist data into the Google Sheet rental dict.
    
    Combines weekly entries (summing if same week exists), merges therapist totals,
    and recomputes all period summaries.
    """
    from datetime import date, timedelta

    # Rebuild full weekly list with start_date/end_date for period filtering
    gs_weekly = []
    for w in rental.get("weekly", []):
        sd = parse_date(w["week"])
        gs_weekly.append({**w, "start_date": sd, "end_date": sd + timedelta(days=6) if sd else sd})

    # Merge: combine DB weeks into GS weeks
    week_index = {w["week"]: w for w in gs_weekly}
    for dbw in db_weekly:
        wk = dbw["week"]
        if wk in week_index:
            # Same week exists in Google Sheet — add DB amounts on top
            existing = week_index[wk]
            existing["total"] = existing.get("total", 0) + dbw["total"]
            existing["cs"] = existing.get("cs", 0) + dbw["cs"]
            existing["ftl"] = existing.get("ftl", 0) + dbw["ftl"]
            existing["pl"] = existing.get("pl", 0) + dbw["pl"]
            existing["mkt"] = existing.get("mkt", 0) + dbw["mkt"]
            existing["testing"] = existing.get("testing", 0) + dbw["testing"]
        else:
            # New week only in DB
            gs_weekly.append(dbw)
            week_index[wk] = dbw

    gs_weekly.sort(key=lambda w: w["week"])

    # Merge therapist totals
    gs_therapists = {(t["name"], t["loc"]): t for t in rental.get("therapists", [])}
    gs_mkt = {(t["name"], t["loc"]): t for t in rental.get("mktTherapists", [])}
    gs_test = {(t["name"], t["loc"]): t for t in rental.get("testTherapists", [])}

    for dbt in db_therapists:
        key = (dbt["name"], dbt["loc"])
        if dbt["loc"] == "MKT":
            target = gs_mkt
        elif dbt["loc"] == "Testing":
            target = gs_test
        else:
            target = gs_therapists

        if key in target:
            target[key]["total"] += dbt["total"]
        else:
            target[key] = {**dbt}

    therapists_all = sorted(list(gs_therapists.values()), key=lambda x: -x["total"])[:60]
    mkt_all = sorted(list(gs_mkt.values()), key=lambda x: -x["total"])
    test_all = sorted(list(gs_test.values()), key=lambda x: -x["total"])

    # Recompute monthly & yearly from merged weekly
    mon_map = defaultdict(lambda: {"gt": 0, "cs": 0, "ftl": 0, "pl": 0, "mkt": 0, "testing": 0, "weeks": 0})
    yr_map = defaultdict(lambda: {"gt": 0, "cs": 0, "ftl": 0, "pl": 0, "mkt": 0, "testing": 0})
    for w in gs_weekly:
        ed = w.get("end_date") or w.get("start_date")
        if not ed:
            continue
        m = ed.strftime("%Y-%m")
        mon_map[m]["gt"] += w.get("total", 0)
        mon_map[m]["cs"] += w.get("cs", 0)
        mon_map[m]["ftl"] += w.get("ftl", 0)
        mon_map[m]["pl"] += w.get("pl", 0)
        mon_map[m]["mkt"] += w.get("mkt", 0)
        mon_map[m]["testing"] += w.get("testing", 0)
        mon_map[m]["weeks"] += 1
        y = str(ed.year)
        yr_map[y]["gt"] += w.get("total", 0)
        yr_map[y]["cs"] += w.get("cs", 0)
        yr_map[y]["ftl"] += w.get("ftl", 0)
        yr_map[y]["pl"] += w.get("pl", 0)
        yr_map[y]["mkt"] += w.get("mkt", 0)
        yr_map[y]["testing"] += w.get("testing", 0)

    rental_monthly = [{"month": k, **{kk: int(vv) for kk, vv in v.items()}} for k, v in sorted(mon_map.items())]
    rental_yearly = [{"year": k, **{kk: int(vv) for kk, vv in v.items()}} for k, v in sorted(yr_map.items())]

    # Clean weekly for output (remove internal date objects)
    weekly_clean = [{k: v for k, v in w.items() if k not in ("start_date", "end_date")} for w in gs_weekly]

    today = date.today()
    cutoff_52 = today - timedelta(weeks=52)
    weekly_52 = [{k: v for k, v in w.items() if k not in ("start_date", "end_date")}
                 for w in gs_weekly if w.get("start_date") and w["start_date"] >= cutoff_52]

    # Period summary helper
    def period_summary(wdata):
        gt = sum(w.get("total", 0) for w in wdata)
        cs = sum(w.get("cs", 0) for w in wdata)
        ftl = sum(w.get("ftl", 0) for w in wdata)
        pl = sum(w.get("pl", 0) for w in wdata)
        mkt = sum(w.get("mkt", 0) for w in wdata)
        testing = sum(w.get("testing", 0) for w in wdata)
        weeks = len(wdata)
        return {"gt": int(gt), "cs": int(cs), "ftl": int(ftl), "pl": int(pl),
                "mkt": int(mkt), "testing": int(testing), "weeks": weeks,
                "avgWeek": int(gt / weeks) if weeks else 0}

    # Period therapist helper — uses DB therapist data filtered by date range
    def period_therapists_merged(wdata, all_therapists, loc_filter=None, n=60):
        # For simplicity, scale therapist totals by ratio of period weeks to total weeks
        # Better: filter by actual week dates, but this requires per-week therapist data
        # For now, use the all-time merged therapist list for all periods
        if loc_filter:
            return sorted([t for t in all_therapists if t["loc"] in loc_filter], key=lambda x: -x["total"])[:n]
        return sorted([t for t in all_therapists if t["loc"] not in ("MKT", "Testing")], key=lambda x: -x["total"])[:n]

    this_year = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == today.year]
    last_year = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == today.year - 1]
    this_month = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == today.year and w["end_date"].month == today.month]
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    this_week = [w for w in gs_weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= week_end and w["end_date"] >= week_start]
    last_week_start = week_start - timedelta(days=7)
    last_week_end = week_start - timedelta(days=1)
    last_week = [w for w in gs_weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= last_week_end and w["end_date"] >= last_week_start]
    today_data = [w for w in gs_weekly if w.get("start_date") and w.get("end_date") and w["start_date"] <= today <= w["end_date"]]
    lm = (date(today.year, today.month, 1) - timedelta(days=1))
    last_month = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == lm.year and w["end_date"].month == lm.month]
    prev_ytd_end = date(today.year - 1, today.month, today.day)
    prev_ytd = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == today.year - 1 and w["end_date"] <= prev_ytd_end]
    prev_ly = [w for w in gs_weekly if w.get("end_date") and w["end_date"].year == today.year - 2]

    # Build per-period therapist lists from DB + GS combined data
    # We need to rebuild these from the per-week DB entries
    def therapists_for_period(period_weeks, all_db_therapists, gs_period_key, loc_filter=None):
        """Combine GS period therapists with DB therapists for matching weeks."""
        gs_list = rental.get(gs_period_key, [])
        period_week_keys = {w["week"] for w in period_weeks}

        # Filter DB therapists to only those whose weeks overlap this period
        db_week_therapists = defaultdict(float)
        for dbt in db_therapists:
            # DB therapists are all-time totals; we need per-period
            # For now, if any DB week falls in this period, include the therapist
            pass

        # Simple approach: use GS period therapists + all DB therapists (since DB has all-time)
        # This is imperfect but workable until full per-week DB therapist tracking
        merged = {}
        for t in gs_list:
            key = (t["name"], t["loc"])
            merged[key] = {**t}

        # Add DB therapists proportionally (scale by period weeks / total weeks)
        total_db_weeks = len(db_weekly) if db_weekly else 1
        period_db_weeks = len([w for w in db_weekly if w["week"] in period_week_keys])
        ratio = period_db_weeks / total_db_weeks if total_db_weeks > 0 else 0

        for dbt in all_db_therapists:
            if loc_filter and dbt["loc"] not in loc_filter:
                continue
            if loc_filter is None and dbt["loc"] in ("MKT", "Testing"):
                continue
            key = (dbt["name"], dbt["loc"])
            scaled = int(dbt["total"] * ratio)
            if scaled <= 0:
                continue
            if key in merged:
                merged[key]["total"] += scaled
            else:
                merged[key] = {**dbt, "total": scaled}

        return sorted(list(merged.values()), key=lambda x: -x["total"])[:60]

    return {
        "weekly": weekly_clean,
        "weekly52": weekly_52,
        "monthly": rental_monthly,
        "yearly": rental_yearly,
        "therapists": therapists_all,
        "mktTherapists": mkt_all,
        "testTherapists": test_all,
        "allTime": period_summary(gs_weekly),
        "ytd": period_summary(this_year),
        "lastYear": period_summary(last_year),
        "thisMonth": period_summary(this_month),
        "thisWeek": period_summary(this_week),
        "lastWeek": period_summary(last_week),
        "today": period_summary(today_data),
        "lastMonth": period_summary(last_month),
        "prevYtd": period_summary(prev_ytd),
        "prevLy": period_summary(prev_ly),
        "ytdTherapists": therapists_for_period(this_year, db_therapists, "ytdTherapists"),
        "lyTherapists": therapists_for_period(last_year, db_therapists, "lyTherapists"),
        "thisMonthTherapists": therapists_for_period(this_month, db_therapists, "thisMonthTherapists"),
        "thisWeekTherapists": therapists_for_period(this_week, db_therapists, "thisWeekTherapists"),
        "lastWeekTherapists": therapists_for_period(last_week, db_therapists, "lastWeekTherapists"),
        "todayTherapists": therapists_for_period(today_data, db_therapists, "todayTherapists"),
        "lastMonthTherapists": therapists_for_period(last_month, db_therapists, "lastMonthTherapists"),
        "mktYtdTherapists": therapists_for_period(this_year, db_therapists, "mktYtdTherapists", loc_filter=["MKT"]),
        "mktLyTherapists": therapists_for_period(last_year, db_therapists, "mktLyTherapists", loc_filter=["MKT"]),
        "mktThisMonthTherapists": therapists_for_period(this_month, db_therapists, "mktThisMonthTherapists", loc_filter=["MKT"]),
        "mktThisWeekTherapists": therapists_for_period(this_week, db_therapists, "mktThisWeekTherapists", loc_filter=["MKT"]),
        "mktLastWeekTherapists": therapists_for_period(last_week, db_therapists, "mktLastWeekTherapists", loc_filter=["MKT"]),
        "testYtdTherapists": therapists_for_period(this_year, db_therapists, "testYtdTherapists", loc_filter=["Testing"]),
        "testLyTherapists": therapists_for_period(last_year, db_therapists, "testLyTherapists", loc_filter=["Testing"]),
        "testThisMonthTherapists": therapists_for_period(this_month, db_therapists, "testThisMonthTherapists", loc_filter=["Testing"]),
        "testThisWeekTherapists": therapists_for_period(this_week, db_therapists, "testThisWeekTherapists", loc_filter=["Testing"]),
        "testLastWeekTherapists": therapists_for_period(last_week, db_therapists, "testLastWeekTherapists", loc_filter=["Testing"]),
    }


def generate_data() -> dict:
    """Fetch both CSVs and return the complete dashboard data dict."""
    today = date.today()
    year_start = date(today.year, 1, 1)
    ly_start = date(today.year - 1, 1, 1)
    ly_end = date(today.year - 1, 12, 31)
    month_start = date(today.year, today.month, 1)
    week_start = today - timedelta(days=today.weekday())

    prev_ytd_start = date(today.year - 1, 1, 1)
    prev_ytd_end = date(today.year - 1, today.month, today.day)
    try:
        prev_month_start = date(today.year if today.month > 1 else today.year - 1,
                                 today.month - 1 if today.month > 1 else 12, 1)
    except Exception:
        prev_month_start = date(today.year - 1, 12, 1)
    prev_month_end = month_start - timedelta(days=1)
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = week_start - timedelta(days=1)
    prev_prev_wk_start = week_start - timedelta(days=14)
    prev_prev_wk_end = week_start - timedelta(days=8)
    yesterday = today - timedelta(days=1)

    prev_ly_start = date(today.year - 2, 1, 1)
    prev_ly_end = date(today.year - 2, 12, 31)

    # Fetch
    lead_rows = fetch_csv(LEAD_CSV_URL)
    rental_rows = fetch_csv(RENTAL_CSV_URL)

    all_leads = process_leads(lead_rows)
    logger.info("Processed %d leads from Google Sheets", len(all_leads))

    # Merge in custom DB leads
    db_leads = convert_db_leads()
    if db_leads:
        # Deduplicate: skip DB leads whose (date, first_name+last_name combo) might already exist
        # For now, simply append — DB leads use the intake form and won't duplicate CSV entries
        all_leads.extend(db_leads)
        logger.info("Total leads after DB merge: %d", len(all_leads))

    # Slice by period
    ytd       = [l for l in all_leads if year_start <= l["date"] <= today]
    lastyear  = [l for l in all_leads if ly_start <= l["date"] <= ly_end]
    month     = [l for l in all_leads if l["date"] >= month_start]
    week      = [l for l in all_leads if l["date"] >= week_start]
    tod       = [l for l in all_leads if l["date"] == today]

    prev_ytd  = [l for l in all_leads if prev_ytd_start <= l["date"] <= prev_ytd_end]
    prev_ly   = [l for l in all_leads if prev_ly_start <= l["date"] <= prev_ly_end]
    prev_mo   = [l for l in all_leads if prev_month_start <= l["date"] <= prev_month_end]
    prev_wk   = [l for l in all_leads if prev_week_start <= l["date"] <= prev_week_end]
    prev_prev_wk = [l for l in all_leads if prev_prev_wk_start <= l["date"] <= prev_prev_wk_end]
    yest      = [l for l in all_leads if l["date"] == yesterday]

    data = {
        "all":       build_period(all_leads),
        "ytd":       build_period(ytd, prev_ytd),
        "lastyear":  build_period(lastyear, prev_ly),
        "month":     build_period(month, prev_mo),
        "week":      build_period(week, prev_wk),
        "lastweek":  build_period(prev_wk, prev_prev_wk),
        "today":     build_period(tod, yest),
        "_monthlyRevenue": build_monthly_revenue(all_leads),
    }

    rental = process_rental(rental_rows)

    # ── Merge DB rental entries into Google Sheet rental data ──
    db_rental_weekly, db_rental_therapists = convert_db_rental()
    if db_rental_weekly:
        rental = merge_rental_data(rental, db_rental_weekly, db_rental_therapists)
        logger.info("Merged DB rental data into dashboard")

    data["_rental"] = rental
    data["_cashflow"] = build_cashflow(all_leads, rental.get("weekly", []))
    data["_generated"] = datetime.now().isoformat()

    logger.info("Data generation complete — %d bytes JSON", len(json.dumps(data)))
    return data
