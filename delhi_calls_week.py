"""
Delhi Inbound Calls — This Week (Mon 24 Mar – Today)
Fetches calls received from Delhi this week, groups by issue/category,
and enriches with transcript data from the local cache.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR   = os.path.dirname(__file__)
ENV_FILE   = os.path.join(BASE_DIR, "env")
CACHE_FILE = os.path.join(BASE_DIR, "transcripts_cache.json")

# This week: Monday 24 Mar 2026 00:00 IST → UTC
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
    if seconds is None:
        return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s"


def fmt_time(ts):
    if ts is None:
        return "-"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ist = ts + timedelta(seconds=19800)
    return ist.strftime("%d %b %Y  %H:%M IST")


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

QUERY = """
SELECT
    sc.id,
    sc.call_initiate_time,
    sc.call_type,
    sc.call_status,
    sc.call_duration,
    sc.callers_call_duration,
    sc.call_issue,
    sc.call_reason,
    sc.call_comments,
    sc.voice_recording_url,
    sc.follow_up,
    sc.call_back,
    sc.priority,
    sc.caller_number,
    ep.first_name || ' ' || ep.last_name        AS agent_name,
    ue.email                                     AS agent_email,
    cp.first_name || ' ' || cp.last_name         AS customer_name,
    cat.name                                     AS call_category,
    subcat.name                                  AS call_subcategory,
    reason.name                                  AS reason_detail,
    gc.name                                      AS city
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


def main():
    env  = load_env(ENV_FILE)
    conn = psycopg2.connect(env["DATABASE_URL"])
    cache = load_cache()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(QUERY, {"week_start": WEEK_START_UTC})
        rows = cur.fetchall()
    conn.close()

    total = len(rows)
    answered = sum(1 for r in rows if r["call_status"] == "answered")
    missed   = sum(1 for r in rows if r["call_status"] == "missed")

    print(f"\n{'='*70}")
    print(f"  DELHI INBOUND CALLS — 24 Mar 2026 (Mon) to Today")
    print(f"{'='*70}")
    print(f"  Total calls : {total}")
    print(f"  Answered    : {answered}")
    print(f"  Missed      : {missed}")
    print()

    # ---------------------------------------------------------------------------
    # Issue / category breakdown
    # ---------------------------------------------------------------------------

    cat_counts   = defaultdict(int)
    subcat_counts = defaultdict(int)
    issue_counts = defaultdict(int)
    reason_counts = defaultdict(int)

    for r in rows:
        cat   = r["call_category"]   or "Unknown"
        sub   = r["call_subcategory"] or "Unknown"
        issue = r["call_issue"]       or "Not specified"
        rsn   = r["reason_detail"]    or "Not specified"
        cat_counts[cat] += 1
        subcat_counts[f"{cat} > {sub}"] += 1
        issue_counts[issue] += 1
        reason_counts[rsn] += 1

    print("--- Call Category Breakdown ---")
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<40} {cnt:>4}")

    print()
    print("--- Sub-category Breakdown ---")
    for sub, cnt in sorted(subcat_counts.items(), key=lambda x: -x[1]):
        print(f"  {sub:<55} {cnt:>4}")

    print()
    print("--- Issue Breakdown ---")
    for issue, cnt in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {issue:<55} {cnt:>4}")

    print()
    print("--- Reason Detail Breakdown ---")
    for rsn, cnt in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {rsn:<55} {cnt:>4}")

    # ---------------------------------------------------------------------------
    # Per-call detail with transcript
    # ---------------------------------------------------------------------------

    print()
    print(f"{'='*70}")
    print("  PER-CALL DETAIL")
    print(f"{'='*70}")

    for r in rows:
        call_id   = str(r["id"])
        transcript = cache.get(call_id, {})
        tx_text    = transcript.get("text") if transcript else None

        print(f"\n[Call #{call_id}]  {fmt_time(r['call_initiate_time'])}")
        print(f"  Status    : {r['call_status']}   Duration: {fmt_duration(r['call_duration'])}")
        print(f"  Agent     : {r['agent_name'] or 'Unassigned'}  ({r['agent_email'] or '-'})")
        print(f"  Customer  : {r['customer_name'] or 'Unknown'}")
        print(f"  Category  : {r['call_category'] or '-'} > {r['call_subcategory'] or '-'}")
        print(f"  Issue     : {r['call_issue'] or '-'}")
        print(f"  Reason    : {r['reason_detail'] or '-'}")
        if r["call_comments"]:
            print(f"  Comments  : {r['call_comments']}")
        if r["follow_up"]:
            print(f"  Follow-up : Yes")
        if r["call_back"]:
            print(f"  Callback  : Yes")
        if tx_text:
            # Show first 400 chars of transcript
            snippet = tx_text[:400].replace("\n", " ")
            if len(tx_text) > 400:
                snippet += "..."
            print(f"  Transcript: {snippet}")
        elif r["voice_recording_url"] and not transcript:
            print(f"  Transcript: [not cached — run with --transcribe to fetch]")


if __name__ == "__main__":
    main()
