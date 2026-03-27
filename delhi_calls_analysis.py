"""
Delhi Inbound Calls Analysis — This Week (Mon 24 Mar – Today)
Uses comments + transcript cache to categorize issues.
"""

import json, os, sys, io
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR   = os.path.dirname(__file__)
ENV_FILE   = os.path.join(BASE_DIR, "env")
CACHE_FILE = os.path.join(BASE_DIR, "transcripts_cache.json")

WEEK_START_IST = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
WEEK_START_UTC = WEEK_START_IST.astimezone(timezone.utc)


def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def fmt_duration(seconds):
    if seconds is None: return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s"


def fmt_time(ts):
    if ts is None: return "-"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ist = ts + timedelta(seconds=19800)
    return ist.strftime("%d %b  %H:%M IST")


QUERY = """
SELECT
    sc.id,
    sc.call_initiate_time,
    sc.call_status,
    sc.call_duration,
    sc.call_comments,
    sc.voice_recording_url,
    sc.follow_up,
    sc.call_back,
    ep.first_name || ' ' || ep.last_name   AS agent_name,
    cp.first_name || ' ' || cp.last_name   AS customer_name,
    cat.name                               AS call_category,
    subcat.name                            AS call_subcategory,
    reason.name                            AS reason_detail,
    gc.name                                AS city
FROM support_customercalls sc
LEFT JOIN users_employee ue       ON sc.employee_id         = ue.id
LEFT JOIN users_person   ep       ON ue.person_id            = ep.id
LEFT JOIN users_customer uc       ON sc.customer_id          = uc.id
LEFT JOIN users_person   cp       ON uc.person_id            = cp.id
LEFT JOIN support_customercallscategory    cat    ON sc.call_category_id     = cat.id
LEFT JOIN support_customercallssubcategory subcat ON sc.call_sub_category_id = subcat.id
LEFT JOIN support_customercallreason       reason ON sc.reason_id            = reason.id
LEFT JOIN geo_city gc             ON sc.city_id              = gc.id
WHERE sc.call_initiate_time >= %(week_start)s
  AND sc.call_type = 'incoming'
  AND LOWER(gc.name) LIKE '%%delhi%%'
ORDER BY sc.call_initiate_time
"""

# Keyword → issue label for comment/transcript classification
KEYWORDS = [
    (["reschedul"], "Rescheduling"),
    (["cancel"], "Cancellation"),
    (["refund", "credit", "money back", "reimburs"], "Refund / Payment"),
    (["bus late", "bus delay", "late", "delay", "not arrived", "not come", "didn't come", "didnt come"], "Bus Delay / Late"),
    (["missed", "missed bus", "miss the bus"], "Missed Bus"),
    (["enquir", "inquiry", "available", "availability", "route", "stop", "timings", "schedule"], "Enquiry / Route Info"),
    (["update", "status", "where is", "where is the bus", "location"], "Bus Location / Status Update"),
    (["complaint", "complain", "rude", "behaviour", "behavior", "misbehav"], "Complaint"),
    (["not answered", "no answer", "unanswered", "didn't pick", "no response"], "Missed / Not Answered"),
    (["subscription", "pass", "plan", "membership"], "Subscription / Pass"),
    (["onboard", "boarding", "pickup", "dropped", "drop"], "Boarding / Drop Issue"),
    (["app", "technical", "otp", "login", "password", "website"], "App / Technical Issue"),
    (["lost", "found", "forgot", "belonging"], "Lost & Found"),
    (["sos", "emergency", "accident", "safety"], "SOS / Emergency"),
    (["feedback", "suggestion", "good", "great", "excellent", "satisfied", "thank"], "Feedback / Appreciation"),
]

def classify(comment, transcript):
    text = ((comment or "") + " " + (transcript or "")).lower()
    for kws, label in KEYWORDS:
        for kw in kws:
            if kw in text:
                return label
    if not text.strip():
        return "No info"
    return "Other"


def main():
    env   = load_env(ENV_FILE)
    conn  = psycopg2.connect(env["DATABASE_URL"])
    cache = load_cache()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(QUERY, {"week_start": WEEK_START_UTC})
        rows = cur.fetchall()
    conn.close()

    total    = len(rows)
    answered = sum(1 for r in rows if r["call_status"] in ("answered", "finished"))
    missed   = sum(1 for r in rows if r["call_status"] == "missed")
    cached   = sum(1 for r in rows if cache.get(str(r["id"]), {}).get("text"))

    print(f"\n{'='*70}")
    print(f"  DELHI INBOUND CALLS — Mon 24 Mar to Thu 26 Mar 2026")
    print(f"{'='*70}")
    print(f"  Total calls       : {total}")
    print(f"  Answered/Finished : {answered}")
    print(f"  Missed            : {missed}")
    print(f"  With transcript   : {cached}")
    print()

    # Classify each call
    classified = []
    for r in rows:
        call_id = str(r["id"])
        tx = cache.get(call_id, {})
        tx_text = tx.get("text") if tx else None
        label = classify(r["call_comments"], tx_text)
        classified.append((r, label, tx_text))

    # Issue counts
    issue_counts = defaultdict(int)
    for _, label, _ in classified:
        issue_counts[label] += 1

    print("--- Issue Breakdown (from comments + transcripts) ---")
    for label, cnt in sorted(issue_counts.items(), key=lambda x: -x[1]):
        bar = "█" * cnt
        print(f"  {label:<35} {cnt:>4}  {bar}")

    # Per-agent breakdown
    print()
    print("--- Per-Agent Volume ---")
    agent_counts = defaultdict(int)
    for r, _, _ in classified:
        agent_counts[r["agent_name"] or "Unassigned"] += 1
    for agent, cnt in sorted(agent_counts.items(), key=lambda x: -x[1]):
        print(f"  {agent:<30} {cnt:>4}")

    # Day-wise breakdown
    print()
    print("--- Day-wise Volume ---")
    day_counts = defaultdict(int)
    for r, _, _ in classified:
        ts = r["call_initiate_time"]
        if ts:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ist_day = (ts + timedelta(seconds=19800)).strftime("%a %d %b")
            day_counts[ist_day] += 1
    for day, cnt in sorted(day_counts.items()):
        print(f"  {day:<15} {cnt:>4}")

    # Missed calls detail
    missed_rows = [(r, l, t) for r, l, t in classified if r["call_status"] == "missed"]
    print()
    print(f"--- Missed Calls ({len(missed_rows)}) ---")
    for r, label, _ in missed_rows:
        cb = "  [CALLBACK REQUESTED]" if r["call_back"] else ""
        print(f"  {fmt_time(r['call_initiate_time'])}  {r['customer_name'] or 'Unknown':<25} {r['agent_name'] or 'Unassigned':<25}{cb}")

    # Full detail per call with transcript snippets
    print()
    print(f"{'='*70}")
    print("  PER-CALL DETAIL (with transcript snippets where available)")
    print(f"{'='*70}")

    for r, label, tx_text in classified:
        call_id = str(r["id"])
        print(f"\n[#{call_id}]  {fmt_time(r['call_initiate_time'])}  |  {r['call_status']}  {fmt_duration(r['call_duration'])}")
        print(f"  Agent     : {r['agent_name'] or 'Unassigned'}")
        print(f"  Customer  : {r['customer_name'] or 'Unknown'}")
        print(f"  Issue Tag : {label}")
        if r["call_comments"]:
            print(f"  Comments  : {r['call_comments']}")
        if r["follow_up"]: print(f"  Follow-up : Yes")
        if r["call_back"]: print(f"  Callback  : Yes")
        if tx_text:
            snippet = tx_text[:500].replace("\n", " ")
            if len(tx_text) > 500: snippet += "..."
            print(f"  Transcript: {snippet}")


if __name__ == "__main__":
    main()
