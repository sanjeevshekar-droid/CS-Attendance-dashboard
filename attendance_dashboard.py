"""
Cityflo CS Associate Attendance Dashboard
------------------------------------------
Data sources (priority order):
  1. IO system  (users_employeelogs DB table) — precise login/break/logout, primary source
  2. Telegram   (CS Internal group)           — fallback for Chats/Sage associates not in IO
  3. Exotel     (support_customercalls DB)    — first-call cross-check for late-login

Break rules:
  Straight shift : max 1 hr total (std 45 min)   → highlight if total personal break > 60 min
  Split shift    : 10 min per session             → highlight if either session > 10 min

Personal breaks counted: Washroom Break, Lunch Break
Work activities NOT counted: Ticket Outbound, Follow Up Outbound, Meeting, Training, Discussion

Usage:
    python attendance_dashboard.py              # today + last 10 days, open browser
    python attendance_dashboard.py --watch      # auto-refresh every 5 min
    python attendance_dashboard.py --interval 3 # custom refresh interval (minutes)
"""

import asyncio, argparse, base64, json, os, re, sys, webbrowser
from collections import defaultdict
from datetime import datetime, date, timezone, timedelta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR    = os.path.dirname(__file__)
ENV_FILE    = os.path.join(BASE_DIR, "env")
ROSTER_FILE = os.path.join(BASE_DIR, "roster.json")
ROSTER_IMG  = os.path.join(BASE_DIR, "roster_413664.jpg")
OUT_FILE    = os.path.join(BASE_DIR, "attendance_dashboard.html")
IST         = timezone(timedelta(hours=5, minutes=30))
HISTORY_DAYS = 10

STRAIGHT_BREAK_MAX  = 3600   # 60 min hard limit
STRAIGHT_BREAK_STD  = 2700   # 45 min standard
SPLIT_BREAK_8H_MAX  = 1200   # 20 min for 8-hour split shift
SPLIT_BREAK_9H_MAX  = 1800   # 30 min for 9-hour split shift

# IO text classification (case-insensitive)
_PERSONAL_BREAK = {
    'washroom break', 'washroom brk', 'lunch break', 'lunch break',
    'lunch', 'break', 'bio break', 'comfort break',
}
_LOGOUT_TEXTS = {
    'logout', 'logged out', 'logged-out', 'disconnected', 'loggedt out',
}
# Work-activity texts (status=False but NOT a personal break, not counted)
_WORK_ACTIVITY = {
    'ticket outbound', 'follow up outbound', 'missed call outbound',
    'meeting with -', 'discussion with -', 'training with -',
    'feedback session', 'outbound', 'outbounds',
}


def _classify_io(status: bool, text: str) -> str:
    """
    Returns: 'login' | 'logout' | 'break' | 'back' | 'work_out' | 'work_back'
    """
    t = (text or '').lower().strip()
    if status:
        return 'login'         # any status=True is "available"
    if t in _LOGOUT_TEXTS:
        return 'logout'
    if t in _PERSONAL_BREAK:
        return 'break'
    if t in _WORK_ACTIVITY or 'outbound' in t or 'meeting' in t or 'training' in t or 'discussion' in t:
        return 'work_out'
    return 'break'             # unknown False → treat as break (conservative)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env

def load_roster():
    with open(ROSTER_FILE, encoding="utf-8") as f:
        return json.load(f)

def _norm(s):
    return re.sub(r"\s+", " ", (s or "").lower().strip())

def fmt_time(dt):
    if dt is None: return "—"
    if dt.tzinfo is None: dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST).strftime("%I:%M %p")

def fmt_secs(dt):
    if dt is None: return "—"
    if dt.tzinfo is None: dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST).strftime("%I:%M:%S %p")

def fmt_dur(secs):
    if not secs or secs <= 0: return "—"
    h, m = divmod(int(secs) // 60, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"

def fmt_delta_plus(secs):
    if secs <= 0: return ""
    h, m = divmod(int(secs) // 60, 60)
    return f"+{h}h {m:02d}m" if h else f"+{m}m"

def make_aware(dt):
    if dt is None: return None
    if dt.tzinfo is None: return dt.replace(tzinfo=IST)
    return dt


# ---------------------------------------------------------------------------
# Roster helpers
# ---------------------------------------------------------------------------
def get_day_associates(roster: dict, target_date) -> list:
    """Returns the associate list for the given date (weekday or weekend roster)."""
    if target_date.weekday() < 5:
        return roster.get("associates", [])
    date_str = str(target_date)
    for wr in roster.get("weekend_rosters", []):
        if wr.get("date") == date_str:
            return wr.get("associates", [])
    return []

def get_day_roster_meta(roster: dict, target_date) -> dict:
    """Returns {associates, image_file, label} for the given date."""
    if target_date.weekday() < 5:
        return {"associates": roster.get("associates", []),
                "image_file": ROSTER_IMG, "label": roster.get("week_label", "")}
    date_str = str(target_date)
    for wr in roster.get("weekend_rosters", []):
        if wr.get("date") == date_str:
            img = os.path.join(BASE_DIR, wr.get("image_file", ""))
            return {"associates": wr.get("associates", []),
                    "image_file": img, "label": wr.get("label", "Weekend Roster")}
    return {"associates": [], "image_file": None, "label": "No weekend roster"}

def match_roster_entry(display_name, roster_or_associates):
    """Accepts either the full roster dict or a list of associate dicts."""
    if isinstance(roster_or_associates, list):
        associates = roster_or_associates
    else:
        associates = roster_or_associates.get("associates", [])
    dn = _norm(display_name)
    for entry in associates:
        for field in ("name_db", "name_tg"):
            val = _norm(entry.get(field, ""))
            if val and (val in dn or dn in val): return entry
        parts = dn.split()
        for field in ("name_db", "name_tg"):
            vp = _norm(entry.get(field, "")).split()
            if parts and vp and parts[0] == vp[0]: return entry
    return None

def expected_times(entry, target_date):
    if not entry: return []
    result = []
    for slot, label in [("morning", "Morning"), ("evening", "Evening")]:
        s = entry.get(slot)
        if not s: continue
        hs, ms = map(int, s["start"].split(":"))
        he, me = map(int, s["end"].split(":"))
        start_dt = datetime(target_date.year, target_date.month, target_date.day, hs, ms, tzinfo=IST)
        end_dt   = datetime(target_date.year, target_date.month, target_date.day, he, me, tzinfo=IST)
        if he < hs: end_dt += timedelta(days=1)
        result.append((start_dt, end_dt, label))
    return result


# ---------------------------------------------------------------------------
# PRIMARY: IO data from users_employeelogs
# ---------------------------------------------------------------------------
def fetch_io_data(target_date: date) -> dict:
    """
    Returns: {employee_id: {
        name, telegram_id, source='io',
        first_login, last_logout, current_status,
        breaks: [{start, end, type, duration_secs}],
        work_events: [{time, text, back_time}],
        raw_events: [(ts, status, text, kind)]
    }}
    """
    try:
        import psycopg2, psycopg2.extras
        env  = load_env(ENV_FILE)
        conn = psycopg2.connect(env["DATABASE_URL"], connect_timeout=10,
                                options="-c statement_timeout=20000")
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT ul.employee_id,
                   ep.first_name||' '||ep.last_name AS name,
                   ue.telegram_id,
                   ul.status,
                   ul.text,
                   ul.created AT TIME ZONE 'Asia/Kolkata' AS ts
            FROM users_employeelogs ul
            JOIN users_employee ue ON ul.employee_id = ue.id
            JOIN users_person   ep ON ue.person_id   = ep.id
            WHERE (ul.created AT TIME ZONE 'Asia/Kolkata')::date = %s
            ORDER BY ul.created
        """, (str(target_date),))
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"  [IO] Warning: {e}")
        return {}

    # Group by employee_id, already in chronological order
    by_emp = defaultdict(list)
    emp_meta = {}
    for r in rows:
        eid = r["employee_id"]
        by_emp[eid].append((make_aware(r["ts"]), r["status"], r["text"] or ""))
        emp_meta[eid] = {"name": r["name"], "telegram_id": r["telegram_id"]}

    result = {}
    for eid, events in by_emp.items():
        meta  = emp_meta[eid]
        rec   = {
            "name": meta["name"], "telegram_id": meta["telegram_id"],
            "source": "io",
            "first_login": None, "last_logout": None,
            "current_status": "absent",
            "breaks": [], "work_events": [], "raw_events": [],
        }

        open_break   = None   # {start, type}
        open_work    = None

        for ts, status, text in events:
            kind = _classify_io(status, text)
            rec["raw_events"].append((ts, status, text, kind))

            if kind == "login":
                if rec["first_login"] is None:
                    rec["first_login"] = ts
                rec["current_status"] = "connected"
                # Close any open break
                if open_break:
                    dur = (ts - open_break["start"]).total_seconds()
                    rec["breaks"].append({
                        "start": open_break["start"], "end": ts,
                        "type": open_break["type"], "duration_secs": int(dur)
                    })
                    open_break = None
                if open_work:
                    rec["work_events"].append({
                        "time": open_work["start"], "text": open_work["type"],
                        "back_time": ts
                    })
                    open_work = None

            elif kind == "break":
                if open_break is None:
                    open_break = {"start": ts, "type": text or "Break"}
                rec["current_status"] = "on_break"

            elif kind == "work_out":
                if open_work is None:
                    open_work = {"start": ts, "type": text or "Work"}
                rec["current_status"] = "working"

            elif kind == "logout":
                rec["last_logout"] = ts
                rec["current_status"] = "disconnected"
                if open_break:
                    dur = (ts - open_break["start"]).total_seconds()
                    rec["breaks"].append({
                        "start": open_break["start"], "end": ts,
                        "type": open_break["type"], "duration_secs": int(dur)
                    })
                    open_break = None
                open_work = None

        # If still open break at end of query (only meaningful for today)
        if open_break:
            rec["_open_break"] = open_break

        result[eid] = rec

    return result


# ---------------------------------------------------------------------------
# SECONDARY: Telegram fallback (for Chats/Sage associates not in IO)
# ---------------------------------------------------------------------------
_RE_LOGIN  = re.compile(r"^\s*connected\b", re.I)
_RE_LOGOUT = re.compile(r"^\s*(disconnected|logged\s*out|loggedt?\s+out)", re.I)
_RE_BREAK  = re.compile(r"^\s*(break|washroom|bio\s*break|lunch)", re.I)
_RE_BACK   = re.compile(r"^\s*in\b", re.I)

def _classify_tg(text):
    if _RE_LOGIN.match(text):  return "login"
    if _RE_LOGOUT.match(text): return "logout"
    if _RE_BREAK.match(text):  return "break"
    if _RE_BACK.match(text) and len(text.split("\n")[0].split()) <= 3: return "back"
    return None

async def fetch_all_tg_messages(client, group_id: int, days_back: int) -> list:
    """
    Single bulk fetch of all Telegram messages for the last `days_back` days.
    Returns list of message objects in chronological order (oldest first).
    Call ONCE per refresh cycle; reuse the list for attendance, swaps, and rosters.
    """
    since_ist = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0) \
                - timedelta(days=days_back - 1)
    raw = []
    async for msg in client.iter_messages(group_id, limit=2000):
        if not msg: continue
        msg_ist = msg.date.astimezone(IST)
        if msg_ist < since_ist:
            break
        raw.append(msg)
    raw.reverse()   # chronological order (oldest first)
    print(f"  [TG] Fetched {len(raw)} messages covering {days_back} days")
    return raw

def tg_attendance_for_date(all_msgs: list, target_date) -> dict:
    """
    Filter pre-fetched messages for one date and build attendance dict.
    No API calls — pure local processing.
    """
    day_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=IST)
    day_end   = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, tzinfo=IST)

    tg_data = {}
    for msg in all_msgs:
        if not msg.text or not msg.sender: continue
        msg_ist = msg.date.astimezone(IST)
        if msg_ist < day_start: continue
        if msg_ist > day_end:   continue

        uid   = msg.sender_id
        fname = (getattr(msg.sender, "first_name", "") or "").strip()
        lname = (getattr(msg.sender, "last_name",  "") or "").strip()
        name  = f"{fname} {lname}".strip() or f"User#{uid}"
        ev    = _classify_tg(msg.text)
        if ev is None: continue

        if uid not in tg_data:
            tg_data[uid] = {"name": name, "source": "telegram",
                            "first_login": None, "last_logout": None,
                            "current_status": "absent",
                            "breaks": [], "work_events": [], "raw_events": []}
        rec = tg_data[uid]
        rec["name"] = name
        rec["raw_events"].append((msg_ist, ev in ("login", "back"), msg.text[:60], ev))
        if ev == "login":
            if rec["first_login"] is None: rec["first_login"] = msg_ist
            rec["current_status"] = "connected"
        elif ev == "logout":
            rec["last_logout"] = msg_ist
            rec["current_status"] = "disconnected"
        elif ev == "break":
            rec["breaks"].append({"start": msg_ist, "end": None, "type": "Break", "duration_secs": 0})
            rec["current_status"] = "on_break"
        elif ev == "back":
            rec["current_status"] = "connected"
            for brk in reversed(rec["breaks"]):
                if brk["end"] is None:
                    brk["end"] = msg_ist
                    brk["duration_secs"] = int((msg_ist - brk["start"]).total_seconds())
                    break
    return tg_data


# ---------------------------------------------------------------------------
# TERTIARY: Exotel first call from DB
# ---------------------------------------------------------------------------
def fetch_exotel(target_date: date) -> dict:
    try:
        import psycopg2, psycopg2.extras
        env  = load_env(ENV_FILE)
        conn = psycopg2.connect(env["DATABASE_URL"], connect_timeout=10,
                                options="-c statement_timeout=20000")
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT ep.first_name||' '||ep.last_name AS agent_name,
                   MIN(sc.call_initiate_time AT TIME ZONE 'Asia/Kolkata') AS first_call,
                   COUNT(*) AS total_calls
            FROM support_customercalls sc
            JOIN users_employee ue ON sc.employee_id = ue.id
            JOIN users_person   ep ON ue.person_id   = ep.id
            WHERE (sc.call_initiate_time AT TIME ZONE 'Asia/Kolkata')::date = %s
              AND sc.call_type IN ('incoming','outgoing')
            GROUP BY ep.first_name, ep.last_name
        """, (str(target_date),))
        rows = cur.fetchall(); conn.close()
        return {_norm(r["agent_name"]): {"first_call": make_aware(r["first_call"]),
                                          "total_calls": r["total_calls"]} for r in rows}
    except Exception as e:
        print(f"  [Exotel] Warning: {e}"); return {}

def find_exotel(name, exotel_data):
    dn = _norm(name)
    for key, rec in exotel_data.items():
        if dn in key or key in dn: return rec
        if dn.split() and key.split() and dn.split()[0] == key.split()[0]: return rec
    return None


# ---------------------------------------------------------------------------
# Merge IO + Telegram + Exotel into unified per-associate records
# ---------------------------------------------------------------------------
def merge_sources(io_data: dict, tg_data: dict, exotel_data: dict,
                  roster: dict, target_date: date) -> list:
    """
    Builds a unified list of associate records.
    IO is primary; Telegram fills in for anyone not in IO.
    Returns list of dicts with keys:
        name, source, first_login, last_logout, current_status,
        breaks, work_events, exotel, roster_entry
    """
    today = datetime.now(IST).date()
    is_today = (target_date == today)
    now_ist  = datetime.now(IST)
    effective_now = now_ist if is_today else datetime(
        target_date.year, target_date.month, target_date.day, 23, 59, 59, tzinfo=IST)

    # Build set of telegram_ids already covered by IO
    io_tg_ids = {rec["telegram_id"] for rec in io_data.values() if rec.get("telegram_id")}

    records   = {}  # key: lower_first_name -> record
    day_assocs = get_day_associates(roster, target_date)

    # 1. All IO records
    for eid, rec in io_data.items():
        key = _norm(rec["name"].split()[0])
        # Close open breaks for today
        if is_today and rec.get("_open_break"):
            ob = rec["_open_break"]
            dur = (now_ist - ob["start"]).total_seconds()
            rec["breaks"].append({
                "start": ob["start"], "end": None,
                "type": ob["type"], "duration_secs": int(dur)
            })
        rec["exotel"] = find_exotel(rec["name"], exotel_data)
        rec["roster_entry"] = match_roster_entry(rec["name"], day_assocs)
        records[key] = rec

    # 2. Telegram records not covered by IO (Chats/Sage associates)
    for tg_uid, rec in tg_data.items():
        if tg_uid in io_tg_ids:
            continue
        key = _norm(rec["name"].split()[0])
        if key in records:
            continue  # already have IO data
        rec["exotel"] = find_exotel(rec["name"], exotel_data)
        rec["roster_entry"] = match_roster_entry(rec["name"], day_assocs)
        # Close open breaks
        if is_today:
            for brk in rec["breaks"]:
                if brk["end"] is None:
                    brk["duration_secs"] = int((now_ist - brk["start"]).total_seconds())
        records[key] = rec

    # 3. Roster members with NO data at all
    for entry in day_assocs:
        key = _norm(entry["name_tg"])
        if key not in records:
            records[key] = {
                "name": entry["name_tg"], "source": "none",
                "first_login": None, "last_logout": None,
                "current_status": "absent",
                "breaks": [], "work_events": [],
                "exotel": find_exotel(entry["name_tg"], exotel_data),
                "roster_entry": entry,
            }

    # Final list sorted: connected first, then break, working, disconnected, absent
    order = {"connected": 0, "on_break": 1, "working": 2, "disconnected": 3, "absent": 4}
    return sorted(records.values(),
                  key=lambda r: (order.get(r["current_status"], 9), r["name"].lower()))


# ---------------------------------------------------------------------------
# Break analysis (works on merged record)
# ---------------------------------------------------------------------------
def _split_shift_hours(roster_entry) -> float:
    """Total scheduled hours for a split-shift associate."""
    total_min = 0
    for slot in ("morning", "evening"):
        s = (roster_entry or {}).get(slot)
        if not s: continue
        hs, ms = map(int, s["start"].split(":"))
        he, me = map(int, s["end"].split(":"))
        total_min += (he * 60 + me) - (hs * 60 + ms)
    return total_min / 60


def analyze_breaks(record: dict, roster_entry, target_date: date, effective_now: datetime) -> dict:
    breaks     = record.get("breaks", [])
    shift_type = (roster_entry or {}).get("type", "straight")
    is_today   = (target_date == datetime.now(IST).date())

    total_personal = 0
    morning_secs   = 0
    evening_secs   = 0

    times          = expected_times(roster_entry, target_date) if roster_entry else []
    morning_window = next(((s, e) for s, e, l in times if l == "Morning"), None)
    evening_window = next(((s, e) for s, e, l in times if l == "Evening"), None)

    for brk in breaks:
        start = make_aware(brk["start"])
        end   = make_aware(brk["end"]) if brk["end"] else (effective_now if is_today else None)
        if end is None: continue
        dur = max(0, (end - start).total_seconds())
        total_personal += dur
        # Also track per-session for display
        if shift_type == "split":
            if morning_window and morning_window[0] <= start < morning_window[1]:
                morning_secs += dur
            elif evening_window and evening_window[0] <= start < evening_window[1]:
                evening_secs += dur
            else:
                if start.hour < 14: morning_secs += dur
                else: evening_secs += dur

    if shift_type == "split":
        shift_hrs = _split_shift_hours(roster_entry)
        max_break = SPLIT_BREAK_9H_MAX if shift_hrs >= 9 else SPLIT_BREAK_8H_MAX
        over = max(0, total_personal - max_break)
        return {
            "type":           "split",
            "shift_hrs":      shift_hrs,
            "max_break":      max_break,
            "total_secs":     int(total_personal),
            "morning_secs":   int(morning_secs),
            "evening_secs":   int(evening_secs),
            "total_exceeded": total_personal > max_break,
            "over_secs":      int(over),
        }
    else:
        over = max(0, total_personal - STRAIGHT_BREAK_MAX)
        return {
            "type":           "straight",
            "total_secs":     int(total_personal),
            "total_exceeded": total_personal > STRAIGHT_BREAK_MAX,
            "over_secs":      int(over),
        }


def on_duty_secs(record: dict, effective_now: datetime, ba: dict) -> int:
    login = record.get("first_login")
    if not login: return 0
    end = make_aware(record.get("last_logout")) or effective_now
    raw = max(0, (end - make_aware(login)).total_seconds())
    return max(0, int(raw - ba.get("total_secs", 0)))


# ---------------------------------------------------------------------------
# Late login
# ---------------------------------------------------------------------------
def compute_late(record: dict, roster: dict, target_date: date, swaps_today: list) -> dict:
    name    = record["name"]
    entry   = record.get("roster_entry") or match_roster_entry(name, roster)
    times   = expected_times(entry, target_date) if entry else []
    grace   = timedelta(minutes=roster.get("grace_minutes", 15))

    exo_rec = record.get("exotel")
    arrival = make_aware(record.get("first_login"))
    exo_time = make_aware(exo_rec["first_call"]) if exo_rec else None

    # Use whichever is earlier — IO login or Exotel first call
    if arrival and exo_time:
        arrival = min(arrival, exo_time)
    elif exo_time:
        arrival = exo_time

    swap_note = ""
    for sw in swaps_today:
        if any(_norm(name).split()[0] in _norm(n) for n in sw.get("names", [])):
            swap_note = sw["text"][:80]; break

    result = {"expected_morning": None, "expected_evening": None,
              "morning_late_secs": 0, "evening_late_secs": 0,
              "arrival": arrival, "exo_time": exo_time,
              "swapped": bool(swap_note), "swap_note": swap_note}

    for exp_start, exp_end, label in times:
        deadline = exp_start + grace
        if label == "Morning":
            result["expected_morning"] = exp_start
            if arrival and arrival.date() == target_date:
                result["morning_late_secs"] = max(0, int((arrival - deadline).total_seconds()))
            elif arrival is None:
                result["morning_late_secs"] = -1
        elif label == "Evening":
            result["expected_evening"] = exp_start
    return result


# ---------------------------------------------------------------------------
# Swap parsing
# ---------------------------------------------------------------------------
def parse_swaps(messages):
    swaps = defaultdict(list)
    for msg_ist, sender, text in messages:
        if not text or "swap" not in text.lower(): continue
        t = text.lower()
        weekdays = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
        if "tomorrow" in t:
            swap_date = msg_ist.date() + timedelta(days=1)
        elif "today" in t:
            swap_date = msg_ist.date()
        else:
            swap_date = msg_ist.date() + timedelta(days=1)
            for i, d in enumerate(weekdays):
                if d in t:
                    days_ahead = (i - msg_ist.date().weekday()) % 7 or 7
                    swap_date = msg_ist.date() + timedelta(days=days_ahead); break
        linked  = re.findall(r'\[([^\]]+)\]', text)
        handles = re.findall(r'@(\w+)', text)
        swaps[swap_date].append({
            "sender": sender, "text": text[:200],
            "names": list(dict.fromkeys([sender] + linked + handles))
        })
    return dict(swaps)

def recent_messages_from_cache(all_msgs: list, days=14) -> list:
    """Extract swap-parseable (date, sender, text) tuples from cached messages."""
    since = datetime.now(IST).date() - timedelta(days=days)
    result = []
    for m in all_msgs:
        if not m.text or not m.sender: continue
        m_ist = m.date.astimezone(IST)
        if m_ist.date() < since: continue
        fname = (getattr(m.sender, "first_name", "") or "").strip()
        result.append((m_ist, fname, m.text))
    return result


# ---------------------------------------------------------------------------
# Auto-fetch weekend roster images from Telegram
# ---------------------------------------------------------------------------
_ROSTER_KW = re.compile(r"(roster|roaster|schedule|shift|weekend|saturday|sunday)", re.I)
_DATE_PAT  = re.compile(
    r"(\d{1,2})\s*(?:st|nd|rd|th)?\s*[&/,]\s*(\d{1,2})\s*(?:st|nd|rd|th)?\s+"
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)",
    re.I
)
_MONTH_MAP = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
              "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

async def fetch_weekend_roster_images(client, all_msgs: list) -> dict:
    """
    Scans cached Telegram messages for weekend roster photos.
    Returns {date_obj: local_image_path}.
    """
    msgs_list = all_msgs  # already filtered to relevant date range

    result = {}  # date -> image_path

    for i, msg in enumerate(msgs_list):
        if not msg.photo: continue
        # Gather context from nearby messages (±3)
        ctx_texts = [m.text or "" for m in msgs_list[max(0,i-3):i+4] if m.text]
        full_ctx  = " ".join(ctx_texts)
        if not _ROSTER_KW.search(full_ctx):
            continue

        msg_ist = msg.date.astimezone(IST)
        yr      = msg_ist.year

        # Try to extract explicit weekend dates from context
        found_dates = []
        m = _DATE_PAT.search(full_ctx)
        if m:
            try:
                mn = _MONTH_MAP[m.group(3)[:3].lower()]
                found_dates = [date(yr, mn, int(m.group(1))),
                               date(yr, mn, int(m.group(2)))]
            except Exception:
                pass

        if not found_dates:
            # Infer from message date
            wd = msg_ist.date().weekday()
            if wd == 5:    # Saturday — this weekend
                sat = msg_ist.date()
            elif wd == 6:  # Sunday — same weekend
                sat = msg_ist.date() - timedelta(days=1)
            else:          # Weekday — upcoming weekend
                days_to_sat = (5 - wd) % 7 or 7
                sat = msg_ist.date() + timedelta(days=days_to_sat)
            found_dates = [sat, sat + timedelta(days=1)]

        for d in found_dates:
            if d.weekday() < 5:  # skip if not actually a weekend
                continue
            fname = os.path.join(BASE_DIR, f"roster_weekend_{d}.jpg")
            if not os.path.exists(fname):
                try:
                    await client.download_media(msg, file=fname)
                    print(f"  [Roster] Downloaded weekend roster for {d} -> {os.path.basename(fname)}")
                except Exception as e:
                    print(f"  [Roster] Download failed for {d}: {e}")
                    continue
            result[d] = fname

    return result


# ---------------------------------------------------------------------------
# Weekly summary aggregation
# ---------------------------------------------------------------------------
def build_weekly_summary(all_days):
    late_logins = []
    exceeded_breaks = []
    not_connected = []

    for d, records in all_days:
        weekday = d.strftime("%a %d %b")
        today = datetime.now(IST).date()
        is_today = (d == today)
        effective_now = datetime.now(IST) if is_today else datetime(
            d.year, d.month, d.day, 23, 59, 59, tzinfo=IST)

        for rec in records:
            name  = rec["name"]
            entry = rec.get("roster_entry")
            ba    = analyze_breaks(rec, entry, d, effective_now)
            late  = compute_late(rec, load_roster(), d, [])

            m_late = late.get("morning_late_secs", 0)
            if m_late and m_late > 0:
                late_logins.append({
                    "date": d, "weekday": weekday, "name": name,
                    "expected": late.get("expected_morning"),
                    "actual": late.get("arrival"),
                    "late_secs": m_late, "swapped": late.get("swapped", False),
                    "source": rec.get("source", "—"),
                })

            if ba.get("total_exceeded"):
                if ba["type"] == "split":
                    shift_h = ba.get("shift_hrs", 8)
                    allowed = fmt_dur(ba.get("max_break", SPLIT_BREAK_8H_MAX))
                    over_str = f"+{fmt_dur(ba['over_secs'])} (limit {allowed}, {shift_h:.0f}h shift)"
                else:
                    over_str = f"+{fmt_dur(ba['over_secs'])}"
                exceeded_breaks.append({
                    "date": d, "weekday": weekday, "name": name,
                    "type": ba["type"], "taken_secs": ba.get("total_secs", 0),
                    "over_str": over_str,
                })

            has_exotel = bool(rec.get("exotel"))
            # Skip absent-marking on weekends unless associate has explicit weekend shifts
            is_weekend = (d.weekday() >= 5)
            if not has_exotel and rec.get("current_status") == "absent" and entry and not is_weekend:
                shift = entry.get("morning") or entry.get("evening")
                if shift:
                    slot = "morning" if entry.get("morning") else "evening"
                    not_connected.append({
                        "date": d, "weekday": weekday, "name": name,
                        "shift": f"{slot.title()} {shift['start']}–{shift['end']}",
                        "type": entry.get("type", "straight"),
                    })

    return {"late_logins": late_logins, "exceeded_breaks": exceeded_breaks,
            "not_connected": not_connected}


# ---------------------------------------------------------------------------
# HTML constants
# ---------------------------------------------------------------------------
STATUS_META = {
    "connected":    ("🟢", "#d4edda", "#155724", "Connected"),
    "on_break":     ("🟡", "#fff3cd", "#856404", "On Break"),
    "working":      ("🔵", "#d1ecf1", "#0c5460", "Work Activity"),
    "disconnected": ("⚫", "#f8f9fa", "#6c757d", "Disconnected"),
    "absent":       ("🔴", "#f8d7da", "#721c24", "Not Logged In"),
}
SOURCE_BADGE = {
    "io":       '<span style="background:#0f3460;color:#fff;font-size:.68rem;padding:1px 5px;border-radius:3px;">IO</span>',
    "telegram": '<span style="background:#229ED9;color:#fff;font-size:.68rem;padding:1px 5px;border-radius:3px;">TG</span>',
    "none":     '',
}

_ROSTER_B64 = None
def roster_img_tag():
    global _ROSTER_B64
    if not os.path.exists(ROSTER_IMG):
        return '<p style="color:#888">Roster image not available.</p>'
    if _ROSTER_B64 is None:
        with open(ROSTER_IMG, "rb") as f:
            _ROSTER_B64 = base64.b64encode(f.read()).decode()
    return f'<img src="data:image/jpeg;base64,{_ROSTER_B64}" style="max-width:100%;border-radius:8px;" alt="Roster">'


# ---------------------------------------------------------------------------
# Break cell HTML
# ---------------------------------------------------------------------------
def break_cell_html(record, ba, effective_now, is_today):
    breaks = record.get("breaks", [])
    work   = record.get("work_events", [])
    parts  = []

    for brk in breaks:
        start = make_aware(brk["start"])
        end   = make_aware(brk["end"]) if brk["end"] else (effective_now if is_today else None)
        if end is None: continue
        dur = int((end - start).total_seconds())
        if dur <= 0: continue
        end_str  = fmt_time(end) if brk["end"] else "ongoing"
        brk_type = brk.get("type", "Break")
        parts.append(f'<span class="break-tag">{brk_type}</span> {fmt_time(start)}–{end_str} <em>({fmt_dur(dur)})</em>')

    # Work activities (show but don't count)
    for ev in work:
        back_str = fmt_time(ev.get("back_time")) if ev.get("back_time") else "—"
        parts.append(f'<span class="work-tag">{ev["text"]}</span> {fmt_time(ev["time"])}–{back_str}')

    individual_str = "<br>".join(parts) if parts else ""

    # Summary line
    if ba["type"] == "split":
        total    = ba["total_secs"]
        max_brk  = ba.get("max_break", SPLIT_BREAK_8H_MAX)
        shift_h  = ba.get("shift_hrs", 8)
        allowed  = fmt_dur(max_brk)
        exceeded = ba.get("total_exceeded", False)
        over_str = f" ⚠️ {fmt_delta_plus(ba['over_secs'])}" if exceeded else ""
        style    = ' class="over"' if exceeded else (' style="color:#e67e22;"' if total > max_brk * 0.8 else "")
        # Per-session breakdown in parentheses
        session_detail = ""
        if ba["morning_secs"] > 0 or ba["evening_secs"] > 0:
            session_detail = f" <small style='color:#888;'>(Morn {fmt_dur(ba['morning_secs'])} · Eve {fmt_dur(ba['evening_secs'])})</small>"
        summary = f'<span{style}>Total: {fmt_dur(total)} / {allowed}{over_str}</span>{session_detail}'
        # label what rule applies
        summary += f' <small style="color:#aaa;">({shift_h:.0f}h shift)</small>'
    else:
        total = ba["total_secs"]
        if ba["total_exceeded"]:
            summary = f'<span class="over">Total: {fmt_dur(total)} ⚠️ {fmt_delta_plus(ba["over_secs"])}</span>'
        elif total > STRAIGHT_BREAK_STD:
            summary = f'<span style="color:#e67e22;">Total: {fmt_dur(total)}</span>'
        elif total > 0:
            summary = f'<span style="color:#888;">Total: {fmt_dur(total)}</span>'
        else:
            summary = ""

    if not parts and not summary:
        return "—"
    if summary:
        return f"{individual_str}<br><small>{summary}</small>" if individual_str else f"<small>{summary}</small>"
    return individual_str


# ---------------------------------------------------------------------------
# Per-day section HTML
# ---------------------------------------------------------------------------
def day_section_html(target_date, records, swaps_today, roster, now_ist, weekend_img_path=None):
    today    = datetime.now(IST).date()
    is_today = (target_date == today)
    effective_now = now_ist if is_today else datetime(
        target_date.year, target_date.month, target_date.day, 23, 59, 59, tzinfo=IST)

    counts = defaultdict(int)
    rows   = []

    for rec in records:
        name   = rec["name"]
        status = rec.get("current_status", "absent")
        counts[status] += 1
        entry  = rec.get("roster_entry")
        ba     = analyze_breaks(rec, entry, target_date, effective_now)
        late   = compute_late(rec, roster, target_date, swaps_today)
        src    = rec.get("source", "none")

        icon, bg, fg, label = STATUS_META.get(status, STATUS_META["absent"])

        login_str  = fmt_secs(rec.get("first_login")) if rec.get("first_login") else "—"
        logout_str = fmt_time(rec.get("last_logout"))

        duty = on_duty_secs(rec, effective_now, ba)

        # Late login cell
        late_parts = []
        exp_m = late.get("expected_morning")
        exp_e = late.get("expected_evening")
        m_late = late.get("morning_late_secs", 0)
        if exp_m:
            if m_late and m_late > 0:
                late_parts.append(f'<span class="late-flag">Exp {fmt_time(exp_m)} · Late {fmt_delta_plus(m_late)}</span>')
            elif m_late == -1:
                late_parts.append(f'<span class="late-absent">Exp {fmt_time(exp_m)} · No-show</span>')
            elif rec.get("first_login"):
                late_parts.append(f'<span class="on-time">Exp {fmt_time(exp_m)} ✓</span>')
            else:
                late_parts.append(f'<span class="muted">Exp {fmt_time(exp_m)}</span>')
        if exp_e:
            late_parts.append(f'<span class="muted">Eve {fmt_time(exp_e)}</span>')
        if late.get("swapped"):
            late_parts.append('<span class="swap-badge">🔀 Swap</span>')
        late_html = "<br>".join(late_parts) or '<span class="muted">—</span>'

        brk_html = break_cell_html(rec, ba, effective_now, is_today)
        platform = (entry or {}).get("platform", "")
        role_str = f'<br><small style="color:#888;">{platform}</small>' if platform else ""
        src_badge = SOURCE_BADGE.get(src, "")

        row_cls = ' class="break-exceeded"' if ba.get("total_exceeded") else ""
        rows.append(f"""
          <tr style="background:{bg};color:{fg};"{row_cls}>
            <td><strong>{name}</strong>{role_str}<br>{src_badge}</td>
            <td style="text-align:center;">{icon} {label}</td>
            <td style="font-size:.8rem;">{login_str}</td>
            <td>{late_html}</td>
            <td>{brk_html}</td>
            <td>{logout_str}</td>
            <td>{fmt_dur(duty)}</td>
          </tr>""")

    swap_html = ""
    if swaps_today:
        items = "".join(f"<li><b>{sw['sender']}</b>: {sw['text']}</li>" for sw in swaps_today)
        swap_html = f'<div class="swap-box"><b>🔀 Shift Swaps Today</b><ul style="margin-top:6px;padding-left:18px;">{items}</ul></div>'

    live_badge   = '<span class="live-badge">● LIVE</span>' if is_today else ""
    is_weekend   = (target_date.weekday() >= 5)
    weekend_note = '<span class="weekend-note">Weekend — roster may not reflect actual schedule</span>' if is_weekend else ""
    date_str     = target_date.strftime("%A, %d %B %Y")
    total_count  = sum(counts.values())

    return f"""
<section id="day-{target_date}" class="day-section" data-date="{target_date}">
  <div class="day-header">
    <div>
      <h2>{date_str} {live_badge}</h2>
      {weekend_note}
      <span class="meta-note">
        {counts.get('connected',0)} connected ·
        {counts.get('on_break',0)} on break ·
        {counts.get('working',0)} work-activity ·
        {counts.get('disconnected',0)} disconnected ·
        {counts.get('absent',0)} absent · {total_count} total
      </span>
    </div>
    <div class="summary-pills">
      <span class="pill green">{counts.get('connected',0)} ●</span>
      <span class="pill yellow">{counts.get('on_break',0)} ⏸</span>
      <span class="pill blue">{counts.get('working',0)} ⚙</span>
      <span class="pill grey">{counts.get('disconnected',0)} ○</span>
      <span class="pill red">{counts.get('absent',0)} ✕</span>
    </div>
  </div>
  {swap_html}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Associate</th><th>Status</th>
          <th>IO Login</th>
          <th>Late Login</th><th>Breaks &amp; Activity</th>
          <th>Logout</th><th>On-Duty</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows) or '<tr><td colspan="7" class="empty-td">No events recorded.</td></tr>'}
      </tbody>
    </table>
  </div>
  {_weekend_roster_block(weekend_img_path, target_date)}
</section>"""


# ---------------------------------------------------------------------------
# Weekly summary HTML
# ---------------------------------------------------------------------------
def weekly_summary_html(summary, week_dates):
    week_label = f"{min(week_dates).strftime('%d %b')} – {max(week_dates).strftime('%d %b %Y')}"

    # Late logins
    if summary["late_logins"]:
        late_rows = "".join(f"""<tr>
          <td>{r['weekday']}</td><td><b>{r['name']}</b></td>
          <td>{fmt_time(r['expected'])}</td><td>{fmt_secs(r['actual'])}</td>
          <td style="color:#c0392b;font-weight:600;">{fmt_delta_plus(r['late_secs'])}</td>
          <td><span style="font-size:.75rem;background:#e9ecef;padding:1px 5px;border-radius:3px;">{r.get('source','').upper()}</span>{'🔀' if r['swapped'] else ''}</td>
        </tr>""" for r in sorted(summary["late_logins"], key=lambda x: (x["date"], x["name"])))
        late_table = f'<table><thead><tr><th>Date</th><th>Associate</th><th>Expected</th><th>Actual</th><th>Late By</th><th>Source</th></tr></thead><tbody>{late_rows}</tbody></table>'
    else:
        late_table = '<p class="empty-msg">No late logins this week 👍</p>'

    # Exceeded breaks
    if summary["exceeded_breaks"]:
        brk_rows = "".join(f"""<tr>
          <td>{r['weekday']}</td><td><b>{r['name']}</b></td>
          <td>{'Split' if r['type']=='split' else 'Straight'}</td>
          <td>{fmt_dur(r['taken_secs'])}</td>
          <td style="color:#c0392b;font-weight:600;">{r['over_str']}</td>
        </tr>""" for r in sorted(summary["exceeded_breaks"], key=lambda x: (x["date"], x["name"])))
        brk_table = f'<table><thead><tr><th>Date</th><th>Associate</th><th>Shift</th><th>Break Taken</th><th>Exceeded By</th></tr></thead><tbody>{brk_rows}</tbody></table>'
    else:
        brk_table = '<p class="empty-msg">No break violations this week 👍</p>'

    # Absent
    if summary["not_connected"]:
        abs_rows = "".join(f"""<tr>
          <td>{r['weekday']}</td><td><b>{r['name']}</b></td>
          <td>{r['shift']}</td><td>{'Split' if r['type']=='split' else 'Straight'}</td>
        </tr>""" for r in sorted(summary["not_connected"], key=lambda x: (x["date"], x["name"])))
        abs_table = f'<table><thead><tr><th>Date</th><th>Associate</th><th>Expected Shift</th><th>Type</th></tr></thead><tbody>{abs_rows}</tbody></table>'
    else:
        abs_table = '<p class="empty-msg">No absentees this week 👍</p>'

    return f"""
<section id="weekly-summary" class="day-section">
  <div class="day-header">
    <div>
      <h2>📊 Weekly Summary — {week_label}</h2>
      <span class="meta-note">Late logins · Exceeded breaks · Absentees · IO + Telegram data</span>
    </div>
    <div class="summary-pills">
      <span class="pill red">{len(summary['late_logins'])} Late</span>
      <span class="pill yellow">{len(summary['exceeded_breaks'])} Break ⚠️</span>
      <span class="pill grey">{len(summary['not_connected'])} Absent</span>
    </div>
  </div>
  <div class="summary-grid">
    <div class="summary-card">
      <div class="card-title">⏰ Late Logins ({len(summary['late_logins'])})</div>
      {late_table}
    </div>
    <div class="summary-card">
      <div class="card-title">☕ Exceeded Breaks ({len(summary['exceeded_breaks'])})</div>
      <div class="break-rules">Straight ≤60 min total · Split 8h=20 min / 9h=30 min total (personal breaks only)</div>
      {brk_table}
    </div>
    <div class="summary-card">
      <div class="card-title">❌ Not Connected / Absent ({len(summary['not_connected'])})</div>
      {abs_table}
    </div>
  </div>
</section>"""


# ---------------------------------------------------------------------------
# Weekend roster image block (embedded inline in day section)
# ---------------------------------------------------------------------------
_WEEKEND_IMG_B64_CACHE = {}

def _weekend_roster_block(img_path, target_date) -> str:
    if not img_path or not os.path.exists(img_path):
        return ""
    global _WEEKEND_IMG_B64_CACHE
    if img_path not in _WEEKEND_IMG_B64_CACHE:
        with open(img_path, "rb") as f:
            _WEEKEND_IMG_B64_CACHE[img_path] = base64.b64encode(f.read()).decode()
    b64 = _WEEKEND_IMG_B64_CACHE[img_path]
    day_name = target_date.strftime("%A %d %b")
    return f"""<div class="roster-card" style="margin-top:14px;">
  <h3>📋 Weekend Roster — {day_name}</h3>
  <img src="data:image/jpeg;base64,{b64}" style="max-width:100%;border-radius:8px;" alt="Weekend Roster">
</div>"""


# ---------------------------------------------------------------------------
# Upcoming swaps section
# ---------------------------------------------------------------------------
def upcoming_swaps_section_html(all_swaps: dict, today: date) -> str:
    future = {d: msgs for d, msgs in sorted(all_swaps.items()) if d > today}
    if not future:
        return ""
    rows = []
    for swap_date, msgs in future.items():
        day_str = swap_date.strftime("%A, %d %b %Y")
        for sw in msgs:
            names_str = ", ".join(sw.get("names", [sw["sender"]]))
            rows.append(f"<li><b>{day_str}</b> — {names_str}: {sw['text'][:160]}</li>")
    if not rows:
        return ""
    return f"""
<div class="upcoming-swaps-card">
  <h3>🔀 Upcoming Swaps ({len(rows)})</h3>
  <ul>{''.join(rows)}</ul>
</div>"""


# ---------------------------------------------------------------------------
# Full page
# ---------------------------------------------------------------------------
CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;background:#f0f2f5;color:#1a1a2e}
.topbar{background:linear-gradient(135deg,#1a1a2e,#0f3460);color:#fff;
        padding:16px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
.topbar h1{font-size:1.4rem;font-weight:700}
.topbar .sub{font-size:.78rem;opacity:.75;margin-top:2px}
.filter-bar{background:#fff;border-radius:10px;padding:14px 20px;
            margin:16px auto 14px;max-width:1350px;
            box-shadow:0 2px 8px rgba(0,0,0,.07);
            display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.filter-bar label{font-size:.83rem;font-weight:600;color:#1a1a2e}
.filter-bar input[type=date],.filter-bar select{border:1px solid #ccc;border-radius:6px;
    padding:6px 11px;font-size:.83rem;cursor:pointer}
.btn{padding:6px 15px;border-radius:6px;border:none;cursor:pointer;font-size:.8rem;font-weight:600}
.btn-today{background:#0f3460;color:#fff}
.btn-week{background:#e9ecef;color:#1a1a2e}
.btn-summary{background:#ffc107;color:#1a1a2e}
.container{max-width:1350px;margin:0 auto;padding:0 14px 40px}
.day-section{margin-bottom:30px;display:none}
.day-section.visible{display:block}
.day-header{display:flex;justify-content:space-between;align-items:flex-start;
            flex-wrap:wrap;gap:10px;margin-bottom:12px}
.day-header h2{font-size:1.08rem;color:#1a1a2e;font-weight:700}
.meta-note{font-size:.73rem;color:#888;display:block;margin-top:2px}
.summary-pills{display:flex;flex-wrap:wrap;gap:6px}
.pill{padding:3px 10px;border-radius:20px;font-size:.74rem;font-weight:700}
.pill.green{background:#d4edda;color:#155724}
.pill.yellow{background:#fff3cd;color:#856404}
.pill.blue{background:#d1ecf1;color:#0c5460}
.pill.grey{background:#e9ecef;color:#6c757d}
.pill.red{background:#f8d7da;color:#721c24}
.live-badge{background:#28a745;color:#fff;font-size:.68rem;font-weight:700;
            padding:2px 7px;border-radius:10px;vertical-align:middle;
            animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.swap-box{background:#fff8e1;border-left:4px solid #ffc107;border-radius:6px;
          padding:10px 14px;margin-bottom:12px;font-size:.82rem}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;background:#fff;
      border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.07)}
thead tr{background:#1a1a2e;color:#fff}
th{padding:10px 12px;text-align:left;font-size:.74rem;
   text-transform:uppercase;letter-spacing:.4px;white-space:nowrap}
td{padding:9px 12px;font-size:.82rem;border-bottom:1px solid rgba(0,0,0,.05);
   vertical-align:top;line-height:1.55}
tr:last-child td{border-bottom:none}
tr.break-exceeded td:nth-child(6){background:rgba(220,53,69,.08)!important}
.empty-td{text-align:center;padding:24px;color:#aaa}
.break-tag{background:#fff3cd;color:#856404;font-size:.72rem;padding:1px 5px;border-radius:3px;margin-right:3px}
.work-tag{background:#d1ecf1;color:#0c5460;font-size:.72rem;padding:1px 5px;border-radius:3px;margin-right:3px}
.over{color:#c0392b;font-weight:600}
.late-flag{color:#c0392b;font-weight:600}
.late-absent{color:#c0392b}
.on-time{color:#1e7e34}
.muted{color:#aaa}
.swap-badge{background:#fff3cd;color:#856404;padding:1px 6px;border-radius:4px;font-size:.74rem}
.summary-grid{display:grid;grid-template-columns:1fr;gap:18px}
@media(min-width:900px){.summary-grid{grid-template-columns:1fr 1fr}}
@media(min-width:1200px){.summary-grid{grid-template-columns:1fr 1fr 1fr}}
.summary-card{background:#fff;border-radius:10px;padding:16px;box-shadow:0 2px 8px rgba(0,0,0,.07)}
.card-title{font-size:.9rem;font-weight:700;margin-bottom:10px;padding-bottom:8px;border-bottom:2px solid #f0f2f5}
.break-rules{font-size:.73rem;color:#888;margin-bottom:10px}
.empty-msg{color:#888;font-size:.83rem;padding:10px 0}
.summary-card table{box-shadow:none;font-size:.78rem}
.summary-card th{font-size:.7rem}
.summary-card td{padding:6px 10px}
.roster-card{background:#fff;border-radius:10px;padding:18px;
             box-shadow:0 2px 8px rgba(0,0,0,.07);margin-top:24px}
.roster-card h3{margin-bottom:10px;font-size:.92rem}
.footer{text-align:center;color:#aaa;font-size:.7rem;padding:16px 0 32px}
.btn-refresh{background:#28a745;color:#fff;}
.cd-wrap{font-size:.75rem;color:#555;display:flex;align-items:center;gap:4px;}
.upcoming-swaps-card{background:#fff8e1;border:1px solid #ffc107;border-radius:10px;
    padding:16px 20px;margin-bottom:22px;box-shadow:0 2px 8px rgba(0,0,0,.06);}
.upcoming-swaps-card h3{font-size:.92rem;margin-bottom:10px;color:#856404;}
.upcoming-swaps-card ul{padding-left:18px;font-size:.82rem;line-height:1.9;}
.upcoming-swaps-card li{border-bottom:1px solid #ffeaa0;padding:3px 0;}
.upcoming-swaps-card li:last-child{border-bottom:none;}
.weekend-note{font-size:.7rem;color:#aaa;font-style:italic;margin-top:4px;}
"""

JS = """
const allDates = Array.from(document.querySelectorAll('.day-section[data-date]'))
                      .map(s=>s.dataset.date).filter(Boolean).sort();
const picker  = document.getElementById('cal-picker');
const viewSel = document.getElementById('view-sel');

function showSection(id){
  document.querySelectorAll('.day-section').forEach(s=>s.classList.remove('visible'));
  const el=document.getElementById(id);
  if(el){el.classList.add('visible');setTimeout(()=>el.scrollIntoView({behavior:'smooth',block:'start'}),80);}
}
function applyFilter(){
  const v=viewSel.value;
  if(v==='summary'){showSection('weekly-summary');return;}
  if(v==='week'){document.querySelectorAll('.day-section').forEach(s=>s.classList.add('visible'));return;}
  showSection('day-'+(picker.value||allDates[allDates.length-1]));
}
document.getElementById('btn-today').addEventListener('click',()=>{picker.value=allDates[allDates.length-1];viewSel.value='single';applyFilter();});
document.getElementById('btn-week').addEventListener('click',()=>{viewSel.value='week';applyFilter();});
document.getElementById('btn-summary').addEventListener('click',()=>{viewSel.value='summary';applyFilter();});
picker.addEventListener('change',()=>{viewSel.value='single';applyFilter();});
viewSel.addEventListener('change',applyFilter);
picker.value=allDates[allDates.length-1];picker.min=allDates[0];picker.max=allDates[allDates.length-1];
applyFilter();

// Refresh countdown
var cd=REFRESH_SECS;
function tick(){
  cd--;
  var m=Math.floor(cd/60),s=cd%60;
  var el=document.getElementById('cd-val');
  if(el) el.textContent=m+':'+(s<10?'0':'')+s;
  if(cd<=0){location.reload();}
}
var cdTimer=setInterval(tick,1000);
document.getElementById('btn-refresh').addEventListener('click',function(){clearInterval(cdTimer);location.reload();});
"""


def build_full_page(day_sections, weekly_html, roster, generated_at, refresh_secs, today, all_swaps=None):
    meta_refresh   = f'<meta http-equiv="refresh" content="{refresh_secs}">'
    week_label     = roster.get("week_label", "Current Week")
    upcoming_html  = upcoming_swaps_section_html(all_swaps or {}, today)
    refresh_min    = refresh_secs // 60
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  {meta_refresh}
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>CS Attendance · {today.strftime('%d %b %Y')}</title>
  <style>{CSS}</style>
</head>
<body>
<div class="topbar">
  <div>
    <h1>CS Associate Attendance Dashboard</h1>
    <div class="sub">Cityflo · Customer Support · IO System + Telegram + Exotel</div>
  </div>
  <div style="text-align:right;font-size:.76rem;opacity:.8;">
    Roster: {week_label}<br>
    Grace: {roster.get('grace_minutes',15)} min ·
    <span style="background:#0f3460;color:#fff;padding:1px 5px;border-radius:3px;font-size:.7rem;">IO</span> primary ·
    <span style="background:#229ED9;color:#fff;padding:1px 5px;border-radius:3px;font-size:.7rem;">TG</span> fallback<br>
    Updated {generated_at.strftime('%d %b %I:%M:%S %p IST')}
  </div>
</div>
<div class="filter-bar">
  <label>Date:</label>
  <input type="date" id="cal-picker">
  <select id="view-sel">
    <option value="single">Single Day</option>
    <option value="week">All Days</option>
    <option value="summary">Weekly Summary</option>
  </select>
  <button class="btn btn-today"   id="btn-today">Today</button>
  <button class="btn btn-week"    id="btn-week">All Days</button>
  <button class="btn btn-summary" id="btn-summary">Weekly Summary</button>
  <span style="flex:1;"></span>
  <button class="btn btn-refresh" id="btn-refresh">⟳ Refresh</button>
  <span class="cd-wrap">Auto in <b id="cd-val">{refresh_min}:00</b></span>
</div>
<div class="container">
  {upcoming_html}
  {weekly_html}
  {''.join(day_sections)}
  <div class="roster-card">
    <h3>📋 Weekly Roster — {week_label}</h3>
    {roster_img_tag()}
  </div>
  <div class="footer">
    IO System (users_employeelogs) · Exotel via DB (support_customercalls) · Telegram CS Internal<br>
    Break rules: Straight ≤60 min total · Split 8h=20 min / 9h=30 min total (personal breaks only)
  </div>
</div>
<script>const REFRESH_SECS={refresh_secs};</script>
<script>{JS}</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def run(args):
    from telethon import TelegramClient
    env      = load_env(ENV_FILE)
    roster   = load_roster()
    API_ID   = int(env["TELEGRAM_API_ID"])
    API_HASH = env["TELEGRAM_API_HASH"]
    SESSION  = os.path.join(BASE_DIR, env.get("TELEGRAM_SESSION_FILE",
                "cityflo_session.session").replace(".session", ""))
    GROUP_ID = int(env["TGID_CS_INTERNAL"])
    REFRESH  = args.interval * 60

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: Telegram session not authorized."); return

    first_run = True
    while True:
        now_ist = datetime.now(IST)
        today   = now_ist.date()
        print(f"\n[{now_ist.strftime('%H:%M:%S')}] Refreshing...", flush=True)

        print("  Fetching Telegram messages (single bulk fetch)...")
        all_tg_msgs  = await fetch_all_tg_messages(client, GROUP_ID, days_back=HISTORY_DAYS + 2)
        recent       = recent_messages_from_cache(all_tg_msgs, days=14)
        swaps        = parse_swaps(recent)
        weekend_imgs = await fetch_weekend_roster_images(client, all_tg_msgs)
        # Seed weekend images already on disk from roster.json entries
        for wr in roster.get("weekend_rosters", []):
            d_str = wr.get("date")
            img   = os.path.join(BASE_DIR, wr.get("image_file", ""))
            if d_str and os.path.exists(img):
                try:
                    weekend_imgs.setdefault(date.fromisoformat(d_str), img)
                except Exception:
                    pass

        dates = [today - timedelta(days=i) for i in range(HISTORY_DAYS - 1, -1, -1)]
        day_sections = []
        all_days     = []

        for d in dates:
            io_data     = fetch_io_data(d)
            tg_data     = tg_attendance_for_date(all_tg_msgs, d)
            exo_data    = fetch_exotel(d)
            records     = merge_sources(io_data, tg_data, exo_data, roster, d)
            swaps_today = swaps.get(d, [])
            w_img       = weekend_imgs.get(d) if d.weekday() >= 5 else None
            print(f"  {d}  IO={len(io_data)} TG={len(tg_data)} Exo={len(exo_data)} total={len(records)}", flush=True)
            day_sections.append(day_section_html(d, records, swaps_today, roster, now_ist, w_img))
            all_days.append((d, records))

        summary = build_weekly_summary(all_days)
        week_dates = [d for d, _ in all_days]
        weekly_html = weekly_summary_html(summary, week_dates)

        html = build_full_page(day_sections, weekly_html, roster, now_ist, REFRESH, today, swaps)
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  Saved -> {OUT_FILE}", flush=True)

        if first_run:
            webbrowser.open(f"file:///{OUT_FILE.replace(chr(92), '/')}")
            first_run = False

        if not args.watch:
            break
        print(f"  Next refresh in {args.interval} min... (Ctrl+C to stop)")
        await asyncio.sleep(REFRESH)

    await client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Cityflo CS Attendance Dashboard")
    parser.add_argument("--watch",    action="store_true", help="Auto-refresh loop")
    parser.add_argument("--interval", type=int, default=5, help="Refresh interval in minutes")
    asyncio.run(run(parser.parse_args()))

if __name__ == "__main__":
    main()
