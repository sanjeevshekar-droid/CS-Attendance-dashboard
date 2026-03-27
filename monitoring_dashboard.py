#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cityflo CS Monitoring Dashboard
================================
Real-time shift-lead view across 6 sections:

  TAB 1 — Associate Performance   (assignments / closures / pending, last 7 days)
  TAB 2 — Ticket Quality Check    (tagged / ops assigned / responded / closed correctly)
  TAB 3 — Released Tickets        (pre-logoff / early releases, valid vs invalid)
  TAB 4 — Backlog & Load          (open count, age buckets, hourly inflow)
  TAB 5 — Auto-Closure Suggest    (silent 24 h, category-based, ghost tickets)
  TAB 6 — Alerts & Flags          (high pending, invalid releases, SLA breaches)

Usage:
    python monitoring_dashboard.py              # generate once and open browser
    python monitoring_dashboard.py --watch 3    # auto-refresh every 3 minutes
"""

import sys, io, json, os, re, webbrowser, argparse, time
import psycopg2
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta, date, time as dtime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ════════════════════════════════════════════════════════════════════════════
# Config
# ════════════════════════════════════════════════════════════════════════════

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
ENV_FILE    = os.path.join(BASE_DIR, 'env')
OUT_FILE    = os.path.join(BASE_DIR, 'monitoring_dashboard.html')
ROSTER_FILE = os.path.join(BASE_DIR, 'roster.json')

IST        = timezone(timedelta(hours=5, minutes=30))
AUDIT_DAYS = 7

# SLA thresholds (hours)
SLA_WARN     = 4
SLA_BREACH   = 8
SLA_CRITICAL = 24

# Release rule: valid window = 1 hour before each associate's shift end (from roster)
# No hardcoded time — derived per-associate from roster.json

# Split shift daily ticket quota — beyond this, releases at logoff are valid
SPLIT_SHIFT_QUOTA = 20

# Shift leads — their releases are always valid regardless of time or quota
# Match is case-insensitive, partial name match
SHIFT_LEAD_NAMES = ['mangesh', 'asha', 'hussain']

# Pending-count alert thresholds per shift type
PENDING_HIGH_FULL  = 20
PENDING_HIGH_SPLIT = 10

SRC_LABEL = {'1': 'App', '7': 'Social', '8': 'IO/WA', '9': 'Sage'}

# Categories that qualify for auto-closure when there's no plain text from customer
AUTO_CLOSE_CATS = {'AC', 'SEAT', 'HYGIENE', 'TRACKING', 'SUGGESTION', 'MENU_ONLY'}

AGENT_PHRASES = [
    'good morning', 'good afternoon', 'good evening', 'we apologize',
    'inconvenience', 'highlighted the issue', 'relevant team', 'allow us',
    'sorry to hear', 'sincerely apologize', 'i have escalated', 'we will check',
    'we will look into', 'we take this seriously', 'please allow', 'kindly allow',
    'we will get back', 'thank you for', 'thanks for reaching', 'we have noted',
    'rest assured', 'we will resolve', 'i hope this helps',
    'we kindly request you to please elaborate', 'kindly elaborate', 'please elaborate',
    'we have shared', 'we have highlighted', 'i have highlighted', 'i have addressed',
    'we will update you', 'we will inform', 'sorry for the inconvenience',
    'we apologize for the inconvenience', 'i am sorry to hear', 'we have escalated',
]

SAGE_NOISE = [
    'cityflo support assistant', 'please choose an option', 'hey there',
    'what can i assist', 'main menu', 'choose from below',
    'how can i help', 'select an option',
]


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def load_env():
    env = {}
    try:
        with open(ENV_FILE, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    env[k.strip()] = v.strip()
    except Exception as e:
        print(f'  WARN: Could not read env: {e}')
    return env


def load_roster():
    try:
        with open(ROSTER_FILE, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _parse_t(s):
    h, m = map(int, s.split(':'))
    return dtime(h, m)


def _norm(s):
    return re.sub(r'\s+', ' ', (s or '').lower().strip())


def get_shift_end(roster, name_db, target_date):
    """Last shift-end datetime (IST) for an associate on a given date."""
    tstr  = target_date.strftime('%Y-%m-%d')
    entry = None
    for wr in roster.get('weekend_rosters', []):
        if wr.get('date') == tstr:
            for a in wr.get('associates', []):
                if _norm(name_db) in _norm(a.get('name_db', '')):
                    entry = a; break
            if entry: break
    if not entry:
        for a in roster.get('associates', []):
            if _norm(name_db) in _norm(a.get('name_db', '')):
                entry = a; break
    if not entry:
        return None
    ends = []
    for sk in ('morning', 'evening'):
        sl = entry.get(sk)
        if sl:
            ends.append(datetime.combine(target_date, _parse_t(sl['end']), tzinfo=IST))
    return max(ends) if ends else None


def get_shift_type(roster, name_db):
    """'split' if the associate has both morning and evening slots, else 'full'."""
    for a in roster.get('associates', []):
        if _norm(name_db) in _norm(a.get('name_db', '')):
            has_m = bool(a.get('morning'))
            has_e = bool(a.get('evening'))
            return 'split' if (has_m and has_e) else 'full'
    return 'full'


def aware(t):
    if t is None: return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def ist(t):
    return aware(t).astimezone(IST) if t else None


def is_agent_reply(comment):
    if not comment: return False
    try:
        json.loads(comment); return False
    except Exception:
        return any(p in comment.lower() for p in AGENT_PHRASES)


def get_yellow_text(text):
    try:
        data = json.loads(text)
        msgs = []
        def walk(obj, in_y=False):
            if isinstance(obj, dict):
                y = obj.get('background', '') == '#FFEEC0' or in_y
                if obj.get('type') == 'Text' and obj.get('value') and y:
                    msgs.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)): walk(v, y)
            elif isinstance(obj, list):
                for item in obj: walk(item, in_y)
        walk(data)
        return ' '.join(msgs)
    except Exception:
        return ''


def is_customer_msg(comment):
    if not comment: return False
    try:
        data = json.loads(comment)
        txt  = get_yellow_text(comment)
        return len(txt.strip()) > 5
    except Exception:
        return not is_agent_reply(comment) and len(comment.strip()) > 8


def label_ticket(cust):
    c = cust.lower()
    if any(k in c for k in [
            'i lost or found', 'earphone', 'earpod', 'earbuds', 'airpod',
            'left my bottle', 'forgot my bottle', 'left my bag', 'forgot my bag',
            'lost my bag', 'left my phone', 'forgot my phone', 'lost my phone',
            'lost my wallet', 'forgot my wallet', 'left my charger', 'left my keys',
            'forgot my keys', 'left my glasses', 'left my umbrella', 'lost one item',
            'pouch on seat', 'tiffin', 'lunch box', 'lunchbox',
            'forgot on seat', 'left on bus', 'left in bus', 'forgot on bus']):
        return 'LOST_FOUND', 'Lost & Found'
    if any(k in c for k in ['issue with driver', 'driving rashly', 'rash driving', 'behaving rudely',
            'rude', 'wrong route', 'took other route', 'unscheduled stop', 'talking on phone']):
        return 'DRIVER', 'Driver Behaviour'
    if any(k in c for k in ['issue with ac', 'ac is not working', 'ac was not working', 'no ac', 'no cooling']):
        return 'AC', 'AC Issue'
    if any(k in c for k in ['my seat has a problem', 'slider is not working', 'handrest is broken',
            'recliner is not working', 'charging point is not working']):
        return 'SEAT', 'Seat / Hardware'
    if any(k in c for k in ['bus quality and hygiene', 'bus was not clean', 'flies in the bus', 'bus broke down']):
        return 'HYGIENE', 'Bus Hygiene'
    if any(k in c for k in ["where is my bus", "can't track", "tracking is wrong", "the bus is not moving"]):
        return 'TRACKING', 'Tracking Issue'
    if any(k in c for k in ['i want to reschedule', 'i missed my bus', 'i want to cancel']):
        return 'RESCHEDULE', 'Reschedule / Cancel'
    if any(k in c for k in ['the bus was late', 'the bus is late', 'bus left early']):
        return 'BUS_TIMING', 'Bus Timing'
    if any(k in c for k in ['payment related', 'i want refund', 'amount deducted', 'charged twice']):
        return 'PAYMENT', 'Payment / Refund'
    if any(k in c for k in ['app issue', 'app not working', 'unable to book', 'login issue']):
        return 'APP', 'App / Booking'
    if any(k in c for k in ['suggestions', 'subscription', 'referral', 'other']):
        return 'SUGGESTION', 'Suggestion'
    if len(c.strip()) < 10:
        return 'MENU_ONLY', 'No Issue Stated'
    return 'UNKNOWN', 'Other / Unclear'


def _esc(s):
    return (str(s).replace('&', '&amp;').replace('<', '&lt;')
            .replace('>', '&gt;').replace('"', '&quot;'))


def _hrs_label(h):
    if h is None: return '—'
    return f'{h:.1f}h' if h < 24 else f'{h/24:.1f}d'


def _age_cls(h):
    if h >= 24: return 'age-crit'
    if h >= 8:  return 'age-warn'
    return ''


# ════════════════════════════════════════════════════════════════════════════
# DB fetch — all sections in one connection
# ════════════════════════════════════════════════════════════════════════════

def fetch_all(conn, roster):
    now_utc  = datetime.now(timezone.utc)
    now_ist  = now_utc.astimezone(IST)
    since    = now_utc - timedelta(days=AUDIT_DAYS + 1)
    today_st = datetime.combine(now_ist.date(), dtime(0, 0), tzinfo=IST).astimezone(timezone.utc)
    cur      = conn.cursor()

    # ── Employee lookup ───────────────────────────────────────────────────
    cur.execute("""
        SELECT e.id, p.first_name, p.last_name, p.user_id
        FROM users_employee e
        JOIN users_person p ON p.id = e.person_id
    """)
    emp_map  = {}   # emp_id → {name, user_id}
    uid_map  = {}   # user_id → emp_id
    for eid, fn, ln, uid in cur.fetchall():
        name = f"{fn or ''} {ln or ''}".strip()
        emp_map[eid]  = {'name': name or f'ID:{eid}', 'user_id': uid}
        if uid: uid_map[int(uid)] = eid

    def emp_name(eid):
        return emp_map.get(eid, {}).get('name', f'ID:{eid}')

    # ── KPI stats ─────────────────────────────────────────────────────────
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status = '1')                       AS open_tickets,
            COUNT(*) FILTER (WHERE created >= %s)                      AS received_today,
            COUNT(*) FILTER (WHERE status != '1' AND modified >= %s)   AS closed_today,
            COUNT(*) FILTER (WHERE status = '1'
                             AND created < NOW() - INTERVAL '24 hours') AS aged_backlog
        FROM support_ticket
    """, (today_st, today_st))
    kpi = dict(zip(['open_tickets','received_today','closed_today','aged_backlog'],
                   cur.fetchone()))

    # ── Section 1: Associate Performance ─────────────────────────────────
    # Assignments (auto + manual): field='Main Ticket Assignee'
    cur.execute("""
        SELECT
            DATE(tc.created AT TIME ZONE 'Asia/Kolkata') AS dt,
            tc.new_value                                  AS assignee_uid,
            tc.author_id                                  AS assigned_by_emp,
            t.source,
            COUNT(*)                                      AS cnt
        FROM support_ticketchange tc
        JOIN support_ticket t ON t.id = tc.ticket_id
        WHERE tc.field = 'Main Ticket Assignee'
          AND tc.new_value IS NOT NULL
          AND tc.new_value ~ '^[0-9]+$'
          AND tc.created >= %s
        GROUP BY 1, 2, 3, 4
        ORDER BY 1 DESC
    """, (since,))
    assign_rows = cur.fetchall()

    # IO tickets (source=8) assigned directly on the ticket
    cur.execute("""
        SELECT
            DATE(t.created AT TIME ZONE 'Asia/Kolkata') AS dt,
            t.assigned_to_employee_id                   AS emp_id,
            COUNT(*)                                    AS cnt
        FROM support_ticket t
        WHERE t.source = '8'
          AND t.assigned_to_employee_id IS NOT NULL
          AND t.created >= %s
        GROUP BY 1, 2
        ORDER BY 1 DESC
    """, (since,))
    io_rows = cur.fetchall()

    # Closures per agent per day
    cur.execute("""
        SELECT
            DATE(tc.created AT TIME ZONE 'Asia/Kolkata') AS dt,
            tc.author_id                                  AS emp_id,
            COUNT(*)                                      AS cnt
        FROM support_ticketchange tc
        WHERE tc.field = 'Status' AND tc.new_value = 'Resolved'
          AND tc.created >= %s
        GROUP BY 1, 2
        ORDER BY 1 DESC
    """, (since,))
    close_rows = cur.fetchall()

    # Current open tickets per associate (pending count)
    cur.execute("""
        SELECT assigned_to_employee_id, COUNT(*) AS cnt
        FROM support_ticket
        WHERE status = '1' AND assigned_to_employee_id IS NOT NULL
        GROUP BY assigned_to_employee_id
    """)
    pending_live = {r[0]: r[1] for r in cur.fetchall()}

    # ── Build daily performance grid ──────────────────────────────────────
    perf = defaultdict(lambda: defaultdict(lambda: {'auto': 0, 'manual': 0, 'io': 0, 'closed': 0}))
    for dt, auid, by_emp, source, cnt in assign_rows:
        try:
            eid  = uid_map.get(int(auid))
        except Exception:
            continue
        if not eid: continue
        name = emp_name(eid)
        dts  = dt.strftime('%d %b %Y') if hasattr(dt, 'strftime') else str(dt)
        kind = 'auto' if (by_emp is None or by_emp == eid) else 'manual'
        perf[dts][name][kind] += cnt

    for dt, eid, cnt in io_rows:
        dts = dt.strftime('%d %b %Y') if hasattr(dt, 'strftime') else str(dt)
        perf[dts][emp_name(eid)]['io'] += cnt

    for dt, eid, cnt in close_rows:
        dts = dt.strftime('%d %b %Y') if hasattr(dt, 'strftime') else str(dt)
        perf[dts][emp_name(eid)]['closed'] += cnt

    # ── Section 2: Ticket Quality Check ──────────────────────────────────
    cur.execute("""
        SELECT
            t.id,
            t.assigned_to_employee_id,
            t.created,
            t.tag_by_cityflo_id IS NOT NULL                          AS is_tagged,
            t.status,
            EXISTS(
                SELECT 1 FROM support_ticketchange tc2
                WHERE tc2.ticket_id = t.id
                  AND tc2.field = 'Ops Assignee'
                  AND tc2.new_value IS NOT NULL
                  AND tc2.new_value NOT IN ('', 'None')
            )                                                        AS has_ops
        FROM support_ticket t
        WHERE t.created >= %s
          AND t.assigned_to_employee_id IS NOT NULL
        ORDER BY t.created DESC
        LIMIT 3000
    """, (since,))
    quality_rows = cur.fetchall()

    quality_tids = [r[0] for r in quality_rows]

    # Fetch comments for quality tickets to detect agent responses
    quality_comments = defaultdict(list)
    if quality_tids:
        ph = ','.join(['%s'] * len(quality_tids))
        cur.execute(f"""
            SELECT ticket_id, comment, is_internal, created
            FROM support_ticketcomment
            WHERE ticket_id IN ({ph})
            ORDER BY ticket_id, created ASC
        """, quality_tids)
        for tid, cmt, is_int, cmt_created in cur.fetchall():
            quality_comments[tid].append((cmt or '', bool(is_int), cmt_created))

    quality = []
    quality_by_agent = defaultdict(lambda: {
        'total': 0, 'tagged': 0, 'ops': 0, 'responded': 0, 'correct': 0
    })

    for tid, eid, created, is_tagged, status, has_ops in quality_rows:
        comments    = quality_comments.get(tid, [])
        pub_cmts    = [(c, ii, t) for c, ii, t in comments if not ii]
        responded   = any(is_agent_reply(c) for c, ii, _ in pub_cmts)
        # "closed correctly" = resolved AND agent responded AND ops assigned
        closed_ok   = (status != '1') and responded and has_ops
        name        = emp_name(eid) if eid else 'Unassigned'
        created_ist = ist(created)

        quality.append({
            'ticket_id': tid,
            'assignee':  name,
            'created':   created_ist.strftime('%d %b %Y %I:%M %p') if created_ist else '—',
            'date_iso':  created_ist.strftime('%Y-%m-%d') if created_ist else '',
            'status':    status,
            'tagged':    is_tagged,
            'ops':       has_ops,
            'responded': responded,
            'closed_ok': closed_ok,
            'score':     sum([is_tagged, has_ops, responded, closed_ok]),
        })

        q = quality_by_agent[name]
        q['total']     += 1
        q['tagged']    += int(is_tagged)
        q['ops']       += int(has_ops)
        q['responded'] += int(responded)
        q['correct']   += int(closed_ok)

    # ── Section 3: Releases (pre-logoff + early releases) ─────────────────
    # ── Section 3: Releases ───────────────────────────────────────────────
    # A release = Spectre Bot (author_id=2044) sets Main Ticket Assignee → NULL.
    # Triggered automatically when an associate logs off.
    #
    # THREE-TIER VALIDITY RULES:
    #
    # Rule A — Shift Lead Exception (highest priority)
    #   Associates: Mangesh, Asha, Hussain
    #   → Always VALID regardless of time or ticket count
    #
    # Rule B — Split Shift Associates (quota-based)
    #   Quota = 20 tickets/day
    #   → VALID if daily assigned count > 20 AND this ticket's rank > 20
    #   → INVALID if daily count ≤ 20 (quota not exceeded)
    #   → INVALID if ticket rank ≤ 20 (core ticket, should have been handled)
    #
    # Rule C — Full Shift Associates (time-window-based)
    #   → VALID if ticket was assigned within the last 1 hour of shift
    #   → INVALID if ticket was assigned before that 1-hour window
    #
    # Deduplication: Spectre logs the same release multiple times per logoff.
    # We keep the EARLIEST record per (ticket, associate) pair.

    SPECTRE_BOT_ID = 2044

    def is_shift_lead(name):
        n = name.lower()
        return any(lead in n for lead in SHIFT_LEAD_NAMES)

    # Step 1: Fetch all deduped releases with assigned_at via lateral join
    cur.execute("""
        WITH deduped_releases AS (
            SELECT DISTINCT ON (ticket_id, old_value)
                ticket_id,
                old_value                               AS assignee_uid,
                created AT TIME ZONE 'Asia/Kolkata'     AS released_ist
            FROM support_ticketchange
            WHERE field     = 'Main Ticket Assignee'
              AND new_value IS NULL
              AND author_id = %s
              AND old_value IS NOT NULL
              AND created   >= %s
            ORDER BY ticket_id, old_value, created ASC
        )
        SELECT
            dr.ticket_id,
            dr.assignee_uid,
            dr.released_ist,
            (
                SELECT tc2.created AT TIME ZONE 'Asia/Kolkata'
                FROM support_ticketchange tc2
                WHERE tc2.ticket_id = dr.ticket_id
                  AND tc2.field     = 'Main Ticket Assignee'
                  AND tc2.new_value = dr.assignee_uid
                  AND tc2.created   < (dr.released_ist AT TIME ZONE 'Asia/Kolkata')
                ORDER BY tc2.created DESC
                LIMIT 1
            )                                           AS assigned_ist,
            p.first_name,
            p.last_name,
            e.id                                        AS emp_id
        FROM deduped_releases dr
        LEFT JOIN users_person   p ON p.user_id::text = dr.assignee_uid
        LEFT JOIN users_employee e ON e.person_id     = p.id
        ORDER BY dr.released_ist DESC
    """, (SPECTRE_BOT_ID, since))
    release_rows = cur.fetchall()

    # Step 2: Build daily assignment rank per (assignee_uid, date)
    # Fetch ALL assignments in the window to determine each ticket's rank in
    # the associate's daily queue (used for split-shift quota check)
    cur.execute("""
        SELECT DISTINCT ON (ticket_id, new_value, DATE(created AT TIME ZONE 'Asia/Kolkata'))
            new_value                                   AS assignee_uid,
            DATE(created AT TIME ZONE 'Asia/Kolkata')   AS dt,
            ticket_id,
            created AT TIME ZONE 'Asia/Kolkata'         AS assigned_ist
        FROM support_ticketchange
        WHERE field     = 'Main Ticket Assignee'
          AND new_value IS NOT NULL
          AND new_value ~ '^[0-9]+$'
          AND created   >= %s
        ORDER BY ticket_id, new_value,
                 DATE(created AT TIME ZONE 'Asia/Kolkata'), created ASC
    """, (since,))

    # daily_queue[(assignee_uid, date_str)] = sorted list of (assigned_ist, ticket_id)
    daily_queue = defaultdict(list)
    for auid, dt, tid, a_ist in cur.fetchall():
        key = (auid, dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt))
        daily_queue[key].append((a_ist, tid))
    for key in daily_queue:
        daily_queue[key].sort(key=lambda x: x[0])   # sort by assignment time

    def get_ticket_rank(assignee_uid, date_str, ticket_id):
        """1-based rank of ticket in associate's daily assignment queue."""
        queue = daily_queue.get((assignee_uid, date_str), [])
        for i, (_, tid) in enumerate(queue):
            if tid == ticket_id:
                return i + 1, len(queue)
        return None, len(queue)

    # Step 3: Classify each release
    releases = []
    for ticket_id, assignee_uid, released_ist, assigned_ist, fn, ln, emp_id in release_rows:
        if released_ist is None: continue

        name = f"{fn or ''} {ln or ''}".strip() or (emp_name(emp_id) if emp_id else f'UID:{assignee_uid}')

        # Normalise to IST
        if released_ist.tzinfo is None:
            released_ist = released_ist.replace(tzinfo=IST)
        released_ist = released_ist.astimezone(IST)
        release_date = released_ist.date()
        date_str     = release_date.strftime('%Y-%m-%d')

        assigned_ist_aware = None
        if assigned_ist:
            if assigned_ist.tzinfo is None:
                assigned_ist = assigned_ist.replace(tzinfo=IST)
            assigned_ist_aware = assigned_ist.astimezone(IST)

        shift_end      = get_shift_end(roster, name, release_date)
        stype          = get_shift_type(roster, name)
        valid_from     = shift_end - timedelta(hours=1) if shift_end else None
        valid_from_str = valid_from.astimezone(IST).strftime('%I:%M %p') if valid_from else '—'
        shift_end_str  = shift_end.astimezone(IST).strftime('%I:%M %p') if shift_end else '—'

        ticket_rank, daily_total = get_ticket_rank(str(assignee_uid), date_str, ticket_id)

        # ── Apply rules in priority order ─────────────────────────────────
        #
        # Priority 1: Shift Lead  → always Exception (Valid)
        # Priority 2: Full Shift  → time-based ONLY (assigned in last 1h of shift)
        # Priority 3: Split Shift → count-based ONLY (rank ≤ 20 = invalid, rank > 20 = valid)

        # Rule A: Shift lead — always valid, regardless of time or count
        if is_shift_lead(name):
            is_invalid = False
            flag       = 'Exception (Valid)'
            rule_used  = 'shift_lead'

        # Rule B: Full shift — time-based ONLY; rank/count are NOT used
        elif stype == 'full':
            if assigned_ist_aware and valid_from:
                is_invalid = assigned_ist_aware < valid_from
                if is_invalid:
                    flag = f'Invalid - Assigned before allowed window (valid from {valid_from_str})'
                else:
                    flag = f'Valid - Within last 1 hour (assigned after {valid_from_str})'
            elif shift_end is None:
                # No roster entry → cannot determine valid window
                is_invalid = False
                flag       = 'Unknown - No roster data'
            else:
                # Shift found but no assignment record → benefit of doubt
                is_invalid = False
                flag       = 'Unknown - No assignment record'
            rule_used = 'full'

        # Rule C: Split shift — count-based ONLY; time is NOT used
        else:
            if ticket_rank is not None and ticket_rank <= SPLIT_SHIFT_QUOTA:
                is_invalid = True
                flag       = f'Invalid - Within first {SPLIT_SHIFT_QUOTA} tickets (rank {ticket_rank})'
            elif ticket_rank is not None and ticket_rank > SPLIT_SHIFT_QUOTA:
                is_invalid = False
                flag       = f'Valid - Beyond {SPLIT_SHIFT_QUOTA} tickets (rank {ticket_rank})'
            else:
                # Rank undetermined (ticket not in daily queue) → benefit of doubt
                is_invalid = False
                flag       = 'Unknown - Rank not determined'
            rule_used = 'split'

        releases.append({
            'ticket_id':     ticket_id,
            'name':          name,
            'shift_type':    'Shift Lead' if is_shift_lead(name) else stype.capitalize(),
            'assigned_str':  assigned_ist_aware.strftime('%d %b %Y %I:%M %p') if assigned_ist_aware else '—',
            'released_str':  released_ist.strftime('%d %b %Y %I:%M %p'),
            'date_iso':      date_str,
            'valid_from':    valid_from_str,
            'shift_end_str': shift_end_str,
            'daily_total':   daily_total,
            'ticket_rank':   ticket_rank or '—',
            'is_invalid':    is_invalid,
            'flag':          flag,
            'rule_used':     rule_used,
        })

    # ── Section 4: Backlog & Load ─────────────────────────────────────────
    cur.execute("""
        SELECT
            CASE
                WHEN NOW() - created < INTERVAL '2 hours'  THEN '< 2h'
                WHEN NOW() - created < INTERVAL '8 hours'  THEN '2–8h'
                WHEN NOW() - created < INTERVAL '24 hours' THEN '8–24h'
                WHEN NOW() - created < INTERVAL '72 hours' THEN '1–3 days'
                ELSE '3+ days'
            END AS bucket,
            CASE
                WHEN NOW() - created < INTERVAL '2 hours'  THEN 1
                WHEN NOW() - created < INTERVAL '8 hours'  THEN 2
                WHEN NOW() - created < INTERVAL '24 hours' THEN 3
                WHEN NOW() - created < INTERVAL '72 hours' THEN 4
                ELSE 5
            END AS sort_order,
            COUNT(*) AS cnt
        FROM support_ticket
        WHERE status = '1'
        GROUP BY 1, 2
        ORDER BY 2
    """)
    backlog_buckets = [{'bucket': r[0], 'cnt': r[2]} for r in cur.fetchall()]

    # Hourly inflow today (IST)
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM (created AT TIME ZONE 'Asia/Kolkata'))::int AS hr,
            COUNT(*) AS cnt
        FROM support_ticket
        WHERE created >= %s
        GROUP BY 1
        ORDER BY 1
    """, (today_st,))
    hourly_inflow = {r[0]: r[1] for r in cur.fetchall()}

    # Hourly closures today
    cur.execute("""
        SELECT
            EXTRACT(HOUR FROM (tc.created AT TIME ZONE 'Asia/Kolkata'))::int AS hr,
            COUNT(*) AS cnt
        FROM support_ticketchange tc
        WHERE tc.field = 'Status' AND tc.new_value = 'Resolved'
          AND tc.created >= %s
        GROUP BY 1
        ORDER BY 1
    """, (today_st,))
    hourly_closed = {r[0]: r[1] for r in cur.fetchall()}

    # ── Section 5: Auto-Closure Suggestions ──────────────────────────────
    cur.execute("""
        SELECT t.id, t.source, t.info_tag, t.created, t.tag_by_cityflo_id,
               t.sageai_category_slug,
               COALESCE(p.first_name || ' ' || p.last_name, 'Unassigned') AS assignee
        FROM support_ticket t
        LEFT JOIN users_employee e ON e.id = t.assigned_to_employee_id
        LEFT JOIN users_person   p ON p.id = e.person_id
        WHERE t.status = '1'
          AND t.created >= NOW() - INTERVAL '60 days'
        ORDER BY t.id DESC
    """)
    open_rows = cur.fetchall()
    open_tids = [r[0] for r in open_rows]

    open_comments = defaultdict(list)
    if open_tids:
        ph = ','.join(['%s'] * len(open_tids))
        cur.execute(f"""
            SELECT ticket_id, comment, is_internal, created
            FROM support_ticketcomment
            WHERE ticket_id IN ({ph})
            ORDER BY ticket_id, created ASC
        """, open_tids)
        for tid, cmt, is_int, tc in cur.fetchall():
            open_comments[tid].append((cmt or '', bool(is_int), tc))

    auto_closure = []
    for tid, source, info_tag, created, tag_id, sage_cat, assignee in open_rows:
        created_a   = aware(created)
        hours_open  = (now_utc - created_a).total_seconds() / 3600
        comments    = open_comments.get(tid, [])
        pub_cmts    = [(c, ii, t) for c, ii, t in comments if not ii]

        # Detect last agent and last customer times
        last_agent = last_cx = None
        for c, ii, t in pub_cmts:
            if is_agent_reply(c):
                if last_agent is None or t > last_agent: last_agent = t
            elif is_customer_msg(c):
                if last_cx is None or t > last_cx: last_cx = t

        last_agent_hrs = round((now_utc - aware(last_agent)).total_seconds()/3600, 1) if last_agent else None
        reason = None

        # Rule 1: Ghost ticket (App source, no comments at all)
        if not pub_cmts and str(source) == '1':
            reason = 'Ghost ticket — no customer message'

        # Rule 2: Silent 24h after agent replied
        elif (last_agent_hrs is not None and
              last_agent_hrs >= 24 and
              (last_cx is None or aware(last_agent) > aware(last_cx))):
            reason = f'Customer silent {last_agent_hrs:.1f}h after agent reply'

        # Rule 3: Category auto-closeable (Sage/App, no real customer text)
        elif str(source) in ('1', '9'):
            cx_text = ''
            for c, ii, _ in pub_cmts:
                if ii: continue
                yellow = get_yellow_text(c)
                if yellow:
                    y = yellow.lower()
                    if not any(n in y for n in SAGE_NOISE):
                        cx_text = yellow; break
                elif not is_agent_reply(c) and len(c.strip()) > 8:
                    cx_text = c; break
            cat_key, _ = label_ticket(cx_text)
            if cat_key in AUTO_CLOSE_CATS and str(source) != '7':
                has_plain = any(
                    not ii and not is_agent_reply(c) and len(c.strip()) > 8
                    for c, ii, _ in pub_cmts
                    if not ii
                )
                try:
                    has_plain = has_plain and not all(
                        bool(json.loads(c)) for c, ii, _ in pub_cmts if not ii and not is_agent_reply(c)
                    )
                except Exception:
                    pass
                if not has_plain:
                    reason = f'Category auto-close: {cat_key}'

        if reason:
            auto_closure.append({
                'ticket_id': tid,
                'assignee':  assignee,
                'created':   ist(created).strftime('%d %b %Y %I:%M %p') if ist(created) else '—',
                'hours_open': round(hours_open, 1),
                'reason':    reason,
                'source':    SRC_LABEL.get(str(source), str(source)),
            })

    auto_closure.sort(key=lambda x: -x['hours_open'])

    # ── Section 6: Alerts ────────────────────────────────────────────────
    # High pending per associate
    alerts_pending = []
    for eid, cnt in pending_live.items():
        name      = emp_name(eid)
        stype     = get_shift_type(roster, name)
        threshold = PENDING_HIGH_FULL if stype == 'full' else PENDING_HIGH_SPLIT
        if cnt >= threshold:
            alerts_pending.append({
                'name': name, 'pending': cnt,
                'shift': stype, 'threshold': threshold,
                'level': 'HIGH' if cnt >= threshold * 1.5 else 'MEDIUM',
            })
    alerts_pending.sort(key=lambda x: -x['pending'])

    # Associates with invalid releases today
    today_str = now_ist.date().strftime('%Y-%m-%d')
    inv_by_agent = defaultdict(list)
    for r in releases:
        if r['is_invalid'] and r['date_iso'] == today_str:
            inv_by_agent[r['name']].append(r['ticket_id'])
    alerts_releases = [
        {'name': k, 'count': len(v), 'ticket_ids': v}
        for k, v in sorted(inv_by_agent.items(), key=lambda x: -len(x[1]))
    ]

    # SLA breaches
    cur.execute("""
        SELECT
            t.id,
            t.created,
            t.assigned_to_employee_id,
            EXTRACT(EPOCH FROM (NOW() - t.created))/3600 AS age_hrs
        FROM support_ticket t
        WHERE t.status = '1'
          AND t.created < NOW() - INTERVAL '%s hours'
        ORDER BY t.created ASC
        LIMIT 500
    """ % SLA_WARN)
    sla_rows = cur.fetchall()

    sla_tickets = []
    for tid, created, eid, age_hrs in sla_rows:
        sla_tickets.append({
            'ticket_id': tid,
            'assignee':  emp_name(eid) if eid else 'Unassigned',
            'created':   ist(created).strftime('%d %b %Y %I:%M %p') if ist(created) else '—',
            'age_hrs':   round(age_hrs, 1),
            'sla_level': ('CRITICAL' if age_hrs >= SLA_CRITICAL else
                          'BREACH'   if age_hrs >= SLA_BREACH else 'WARNING'),
        })

    # ── Daily KPI (per-day received/closed, for date-range KPI cards) ────────
    cur.execute("""
        SELECT DATE(created AT TIME ZONE 'Asia/Kolkata') AS dt, COUNT(*) AS cnt
        FROM support_ticket
        WHERE created >= %s
        GROUP BY 1 ORDER BY 1
    """, (since,))
    _daily_recv = {(r[0].strftime('%Y-%m-%d') if hasattr(r[0],'strftime') else str(r[0])): r[1]
                   for r in cur.fetchall()}

    cur.execute("""
        SELECT DATE(tc.created AT TIME ZONE 'Asia/Kolkata') AS dt, COUNT(*) AS cnt
        FROM support_ticketchange tc
        WHERE tc.field = 'Status' AND tc.new_value = 'Resolved'
          AND tc.created >= %s
        GROUP BY 1 ORDER BY 1
    """, (since,))
    _daily_clos = {(r[0].strftime('%Y-%m-%d') if hasattr(r[0],'strftime') else str(r[0])): r[1]
                   for r in cur.fetchall()}

    all_dates = sorted(set(list(_daily_recv) + list(_daily_clos)))
    daily_kpi_list = [{'date': d, 'received': _daily_recv.get(d, 0), 'closed': _daily_clos.get(d, 0)}
                      for d in all_dates]

    # ── Flat perf list (for JS rendering) ────────────────────────────────────
    perf_flat = []
    for dts, agents in perf.items():
        try:
            iso = datetime.strptime(dts, '%d %b %Y').strftime('%Y-%m-%d')
        except Exception:
            continue
        for name, counts in agents.items():
            stype = get_shift_type(roster, name)
            perf_flat.append({
                'date': iso, 'label': dts, 'name': name,
                'stype': stype,
                'auto': counts.get('auto', 0), 'manual': counts.get('manual', 0),
                'io': counts.get('io', 0), 'closed': counts.get('closed', 0),
            })

    # ── Assignment summary per (associate, date) for split-shift tracking ────
    def _is_shift_lead(name):
        n = name.lower()
        return any(lead in n for lead in SHIFT_LEAD_NAMES)

    assign_sum_dict = {}

    for (auid, date_str), queue in daily_queue.items():
        try:
            uid_int = int(auid)
        except Exception:
            continue
        eid = uid_map.get(uid_int)
        if not eid:
            continue
        name  = emp_name(eid)
        stype = 'Shift Lead' if _is_shift_lead(name) else get_shift_type(roster, name).capitalize()
        key   = (name, date_str)
        assign_sum_dict.setdefault(key, {
            'name': name, 'date': date_str, 'stype': stype,
            'total': 0, 'completed': 0, 'released': 0,
            'within_released': 0, 'beyond_released': 0,
        })['total'] = len(queue)

    for r in releases:
        key   = (r['name'], r['date_iso'])
        stype = r['shift_type']
        if key not in assign_sum_dict:
            assign_sum_dict[key] = {
                'name': r['name'], 'date': r['date_iso'], 'stype': stype,
                'total': r['daily_total'], 'completed': 0, 'released': 0,
                'within_released': 0, 'beyond_released': 0,
            }
        assign_sum_dict[key]['released'] += 1
        rank = r['ticket_rank']
        if stype == 'Split' and isinstance(rank, int):
            if rank <= SPLIT_SHIFT_QUOTA:
                assign_sum_dict[key]['within_released'] += 1
            else:
                assign_sum_dict[key]['beyond_released'] += 1
        else:
            assign_sum_dict[key]['within_released'] += 1

    for row in perf_flat:
        key = (row['name'], row['date'])
        if key in assign_sum_dict:
            assign_sum_dict[key]['completed'] += row['closed']

    assign_summary = sorted(assign_sum_dict.values(),
                            key=lambda x: (x['date'], x['name']), reverse=True)

    # ── Add date field to auto_closure and sla_tickets ────────────────────────
    for r in auto_closure:
        # parse 'DD Mon YYYY HH:MM AM/PM' → ISO date
        try:
            r['date'] = datetime.strptime(r['created'], '%d %b %Y %I:%M %p').strftime('%Y-%m-%d')
        except Exception:
            r['date'] = now_ist.date().strftime('%Y-%m-%d')

    for r in sla_tickets:
        try:
            r['date'] = datetime.strptime(r['created'], '%d %b %Y %I:%M %p').strftime('%Y-%m-%d')
        except Exception:
            r['date'] = now_ist.date().strftime('%Y-%m-%d')

    # Pending live: convert to {name: count} for JS
    pending_by_name = {emp_name(eid): cnt for eid, cnt in pending_live.items()}

    cur.close()

    return {
        'kpi':            kpi,
        'daily_kpi':      daily_kpi_list,
        'perf_flat':      perf_flat,
        'perf':           dict(perf),
        'quality':        quality,
        'quality_agent':  dict(quality_by_agent),
        'releases':       releases,
        'assign_summary': assign_summary,
        'backlog_buckets': backlog_buckets,
        'hourly_inflow':  hourly_inflow,
        'hourly_closed':  hourly_closed,
        'auto_closure':   auto_closure,
        'alerts_pending': alerts_pending,
        'alerts_releases': alerts_releases,
        'sla_tickets':    sla_tickets,
        'pending_live':   pending_live,
        'pending_by_name': pending_by_name,
        'emp_map':        emp_map,
        'now_utc':        now_utc,
        'now_ist':        now_ist,
        'roster':         roster,
    }


# ════════════════════════════════════════════════════════════════════════════
# HTML + JS renderer  (all rendering is in JavaScript; Python only embeds JSON)
# ════════════════════════════════════════════════════════════════════════════

def build_html(data, refresh_secs=0):
    import json as _json
    from decimal import Decimal

    class _Enc(_json.JSONEncoder):
        def default(self, o):
            if isinstance(o, Decimal):
                return float(o)
            return super().default(o)

    def _dumps(obj):
        return _json.dumps(obj, cls=_Enc)

    now_ist   = data['now_ist']
    kpi       = data['kpi']
    ts        = now_ist.strftime('%d %b %Y  %I:%M:%S %p IST')
    today_iso = now_ist.date().strftime('%Y-%m-%d')
    week_ago  = (now_ist.date() - timedelta(days=6)).strftime('%Y-%m-%d')

    # ── Serialize releases ───────────────────────────────────────────────────
    rel_js = _dumps([{
        'tid':     r['ticket_id'],
        'name':    r['name'],
        'stype':   r['shift_type'],
        'assigned':r['assigned_str'],
        'released':r['released_str'],
        'date':    r['date_iso'],
        'vfrom':   r['valid_from'],
        'shend':   r['shift_end_str'],
        'dtotal':  r['daily_total'],
        'rank':    r['ticket_rank'] if isinstance(r['ticket_rank'], int) else -1,
        'inv':     r['is_invalid'],
        'flag':    r['flag'],
        'rule':    r['rule_used'],
    } for r in data['releases']])

    # ── Serialize quality ────────────────────────────────────────────────────
    qual_js = _dumps([{
        'tid':     q['ticket_id'],
        'name':    q['assignee'],
        'date':    q['date_iso'],
        'created': q['created'],
        'tagged':  q['tagged'],
        'ops':     q['ops'],
        'resp':    q['responded'],
        'closedok':q['closed_ok'],
        'score':   q['score'],
    } for q in data['quality']])

    # ── Serialize performance (flat) ─────────────────────────────────────────
    perf_js = _dumps(data['perf_flat'])

    # ── Serialize assignment summary ─────────────────────────────────────────
    asum_js = _dumps(data['assign_summary'])

    # ── Serialize auto-closure ───────────────────────────────────────────────
    ac_js = _dumps([{
        'tid':    r['ticket_id'],
        'source': r['source'],
        'name':   r['assignee'],
        'created':r['created'],
        'date':   r['date'],
        'hrs':    float(r['hours_open']) if r['hours_open'] is not None else None,
        'reason': r['reason'],
    } for r in data['auto_closure']])

    # ── Serialize SLA tickets ────────────────────────────────────────────────
    sla_js = _dumps([{
        'tid':   r['ticket_id'],
        'name':  r['assignee'],
        'created':r['created'],
        'date':  r['date'],
        'hrs':   float(r['age_hrs']) if r['age_hrs'] is not None else None,
        'level': r['sla_level'],
    } for r in data['sla_tickets']])

    # ── Serialize daily KPI ──────────────────────────────────────────────────
    dkpi_js = _dumps(data['daily_kpi'])

    # ── Serialize backlog + hourly (static/current-state) ───────────────────
    bl_js      = _dumps(data['backlog_buckets'])
    h_inflow   = _dumps([data['hourly_inflow'].get(h, 0) for h in range(24)])
    h_closed   = _dumps([data['hourly_closed'].get(h, 0) for h in range(24)])
    pending_js = _dumps(data['pending_by_name'])

    # ── Alerts pending + releases (current-day, computed in JS) ─────────────
    ap_js = _dumps([{
        'name': r['name'], 'pending': r['pending'],
        'shift': r['shift'], 'threshold': r['threshold'], 'level': r['level'],
    } for r in data['alerts_pending']])

    # ── Static live KPI values ───────────────────────────────────────────────
    live_kpi_js = _dumps({
        'open':    kpi['open_tickets'],
        'aged':    kpi['aged_backlog'],
    })

    auto_reload = (f'<script>setTimeout(()=>location.reload(),{refresh_secs*1000});</script>'
                   if refresh_secs > 0 else '')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cityflo CS Monitoring Dashboard</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
body{{font-family:system-ui,sans-serif;background:#f4f6f9;font-size:13.5px}}
.top-bar{{background:#1a1a2e;color:#eee;padding:10px 20px;display:flex;justify-content:space-between;align-items:center}}
.top-bar h1{{font-size:1.05rem;margin:0;font-weight:600;color:#fff}}
.top-bar .ts{{font-size:.76rem;color:#adb5bd}}
.date-bar{{background:#fff;border-bottom:1px solid #e0e0e0;padding:8px 20px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;position:sticky;top:0;z-index:100;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
.date-bar label{{font-size:12px;font-weight:600;color:#495057;margin:0}}
.date-bar input[type=date]{{border:1px solid #ced4da;border-radius:6px;padding:4px 8px;font-size:12px;cursor:pointer}}
.date-bar .quick-btn{{background:#f0f2f5;border:1px solid #dee2e6;border-radius:6px;padding:3px 10px;font-size:12px;cursor:pointer;transition:.15s}}
.date-bar .quick-btn:hover,.date-bar .quick-btn.active{{background:#0d6efd;color:#fff;border-color:#0d6efd}}
.date-bar .err{{color:#dc3545;font-size:12px;font-weight:600}}
.kpi-grid{{display:grid;gap:12px;padding:14px 20px;grid-template-columns:repeat(4,1fr)}}
.kpi-card{{background:#fff;border-radius:10px;padding:14px 18px;box-shadow:0 1px 4px rgba(0,0,0,.08);text-align:center}}
.kpi-val{{font-size:2rem;font-weight:700}}
.kpi-lbl{{font-size:.72rem;color:#6c757d;text-transform:uppercase;letter-spacing:.5px}}
.kpi-sub{{font-size:.7rem;color:#adb5bd;margin-top:2px}}
.tab-area{{padding:12px 20px}}
.data-table{{width:100%;border-collapse:collapse;font-size:13px}}
.data-table th{{background:#f0f2f5;font-weight:600;padding:8px 10px;text-align:left;position:sticky;top:0;z-index:1}}
.data-table td{{padding:7px 10px;border-bottom:1px solid #f0f0f0;vertical-align:middle}}
.data-table tr:hover{{background:#fafafa}}
.tc{{text-align:center}}
.tw{{white-space:normal;word-break:break-word}}
.table-wrap{{overflow:auto;border-radius:8px;border:1px solid #e9ecef;max-height:460px}}
.age-warn{{color:#fd7e14;font-weight:600}}
.age-crit{{color:#dc3545;font-weight:700}}
.badge-src{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.7rem;font-weight:600}}
.src-app{{background:#cfe2ff;color:#084298}}
.src-social{{background:#fce8d0;color:#7d3c00}}
.src-iowa,.src-io,.src-iowa{{background:#d1f0e8;color:#0a5c40}}
.src-sage{{background:#ede8fb;color:#4b2b8c}}
.chart-card{{background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
.section-wrap{{background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07);padding:16px}}
.filter-row{{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap}}
.no-data{{text-align:center;color:#adb5bd;padding:24px;font-style:italic}}
::-webkit-scrollbar{{width:6px;height:6px}}
::-webkit-scrollbar-track{{background:#f1f1f1}}
::-webkit-scrollbar-thumb{{background:#ccc;border-radius:3px}}
.bar-wrap{{background:#e9ecef;border-radius:4px;height:10px;width:100%}}
.bar-fill{{height:10px;border-radius:4px}}
</style>
</head>
<body>

<!-- Top bar -->
<div class="top-bar">
  <h1>Cityflo CS — Monitoring Dashboard</h1>
  <span class="ts">Last updated: {ts}{'&nbsp;|&nbsp;Auto-refresh every ' + str(refresh_secs//60) + ' min' if refresh_secs else ''}</span>
</div>

<!-- Date filter bar -->
<div class="date-bar">
  <label>From</label>
  <input type="date" id="startDate" value="{week_ago}">
  <label>To</label>
  <input type="date" id="endDate"   value="{today_iso}">
  <button class="quick-btn" onclick="setQ('today')">Today</button>
  <button class="quick-btn" onclick="setQ('yesterday')">Yesterday</button>
  <button class="quick-btn active" id="btn7d" onclick="setQ('7d')">Last 7 Days</button>
  <button class="quick-btn" onclick="setQ('30d')">Last 30 Days</button>
  <button class="quick-btn" onclick="setQ('all')">All Data</button>
  <span id="dateErr" class="err"></span>
  <span id="rangeLabel" style="font-size:12px;color:#6c757d;margin-left:4px"></span>
</div>

<!-- Live KPI strip (open & aged are always current-state) -->
<div class="kpi-grid">
  <div class="kpi-card"><div class="kpi-val" id="kv-open" style="color:#dc3545">{kpi['open_tickets']}</div><div class="kpi-lbl">Open Now</div><div class="kpi-sub">Current state</div></div>
  <div class="kpi-card"><div class="kpi-val" id="kv-recv" style="color:#fd7e14">—</div><div class="kpi-lbl">Received in Range</div><div class="kpi-sub" id="kv-recv-sub"></div></div>
  <div class="kpi-card"><div class="kpi-val" id="kv-clos" style="color:#28a745">—</div><div class="kpi-lbl">Closed in Range</div><div class="kpi-sub" id="kv-clos-sub"></div></div>
  <div class="kpi-card"><div class="kpi-val" id="kv-aged" style="color:#6f42c1">{kpi['aged_backlog']}</div><div class="kpi-lbl">Aged Backlog (24h+)</div><div class="kpi-sub">Current state</div></div>
</div>

<!-- Tabs -->
<div class="tab-area">
  <ul class="nav nav-tabs mb-0" id="mainTabs">
    <li class="nav-item"><a class="nav-link active" data-bs-toggle="tab" href="#t1">Performance</a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t2">Quality</a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t3">Releases <span id="badge-inv" class="badge bg-danger ms-1"></span></a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t4">Assignment Tracking <span class="badge bg-secondary ms-1">Split</span></a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t5">Backlog &amp; Load</a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t6">Auto-Closure <span id="badge-ac" class="badge bg-warning text-dark ms-1"></span></a></li>
    <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#t7">Alerts <span id="badge-al" class="badge bg-danger ms-1"></span></a></li>
  </ul>
  <div class="tab-content mt-2">
    <div class="tab-pane fade show active section-wrap" id="t1">
      <div class="filter-row"><input type="text" id="f-perf" class="form-control form-control-sm" placeholder="Filter associate…" style="max-width:200px"></div>
      <h6>7-Day Summary Per Associate</h6>
      <div class="table-wrap"><table class="data-table"><thead id="perf-sum-head"></thead><tbody id="perf-sum-body"></tbody></table></div>
      <h6 class="mt-3">Daily Breakdown</h6>
      <div class="table-wrap"><table class="data-table"><thead id="perf-day-head"></thead><tbody id="perf-day-body"></tbody></table></div>
    </div>
    <div class="tab-pane fade section-wrap" id="t2">
      <div class="filter-row"><input type="text" id="f-qual" class="form-control form-control-sm" placeholder="Filter associate…" style="max-width:200px"></div>
      <h6>Quality Score Per Associate</h6>
      <p class="text-muted small mb-2">Checks: Issue Tagged · Ops Assignee · Agent Responded · Closed Correctly</p>
      <div class="table-wrap"><table class="data-table"><thead id="qual-sum-head"></thead><tbody id="qual-sum-body"></tbody></table></div>
      <h6 class="mt-3">Failing Tickets <small class="text-muted fw-normal" id="qual-fail-count"></small></h6>
      <div class="table-wrap"><table class="data-table"><thead id="qual-fail-head"></thead><tbody id="qual-fail-body"></tbody></table></div>
    </div>
    <div class="tab-pane fade section-wrap" id="t3">
      <div class="alert alert-info py-2 small mb-2">
        <strong>Releases = Spectre Bot unassigns (Main Ticket Assignee → None) on logoff.</strong><br>
        <span class="badge bg-info text-dark">Shift Lead</span> Exception (Valid) always &nbsp;|&nbsp;
        <span class="badge bg-warning text-dark">Split</span> Rank ≤ {SPLIT_SHIFT_QUOTA} = Invalid · Rank &gt; {SPLIT_SHIFT_QUOTA} = Valid &nbsp;|&nbsp;
        <span class="badge bg-primary">Full</span> Valid only if assigned within last 1h of shift end
      </div>
      <div class="kpi-grid mb-2" style="grid-template-columns:repeat(4,1fr);padding:0">
        <div class="kpi-card"><div class="kpi-val" id="rv-total">—</div><div class="kpi-lbl">Total Releases</div></div>
        <div class="kpi-card"><div class="kpi-val" id="rv-inv" style="color:#dc3545">—</div><div class="kpi-lbl">Invalid</div><div class="kpi-sub" id="rv-inv-pct"></div></div>
        <div class="kpi-card"><div class="kpi-val" id="rv-val" style="color:#28a745">—</div><div class="kpi-lbl">Valid</div></div>
        <div class="kpi-card"><div class="kpi-val" id="rv-sl" style="color:#0dcaf0">—</div><div class="kpi-lbl">Shift Lead</div></div>
      </div>
      <div class="filter-row"><input type="text" id="f-rel" class="form-control form-control-sm" placeholder="Filter associate…" style="max-width:200px"></div>
      <h6>Summary Per Associate</h6>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Associate</th><th>Shift Type</th><th class="tc">Total</th><th class="tc">Invalid</th><th class="tc">Valid</th><th class="tc">Daily Assigned</th><th>Invalid Ticket IDs</th></tr></thead><tbody id="rel-sum-body"></tbody></table></div>
      <h6 class="mt-3">Invalid Release Details <small class="text-muted fw-normal" id="rel-inv-count"></small></h6>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Ticket #</th><th>Associate</th><th>Shift</th><th>Assigned At</th><th>Released At</th><th class="tc">Daily Total</th><th class="tc">Rank</th><th class="tc">Valid From</th><th class="tc">Shift End</th><th>Validation Reason</th></tr></thead><tbody id="rel-inv-body"></tbody></table></div>
      <h6 class="mt-3">Valid Releases <small class="text-muted fw-normal" id="rel-val-count"></small></h6>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Ticket #</th><th>Associate</th><th>Shift</th><th>Assigned At</th><th>Released At</th><th class="tc">Daily Total</th><th class="tc">Rank</th><th class="tc">Valid From</th><th class="tc">Shift End</th><th>Validation Reason</th></tr></thead><tbody id="rel-val-body"></tbody></table></div>
    </div>
    <div class="tab-pane fade section-wrap" id="t4">
      <div class="alert alert-secondary py-2 small mb-2">
        Assignment tracking for split-shift associates (quota = {SPLIT_SHIFT_QUOTA} tickets/day).
        Tickets ranked by assignment time (FIFO). Rank ≤ {SPLIT_SHIFT_QUOTA} = Within Limit (must complete). Rank &gt; {SPLIT_SHIFT_QUOTA} = Beyond Limit (may release).
      </div>
      <div class="filter-row"><input type="text" id="f-asum" class="form-control form-control-sm" placeholder="Filter associate…" style="max-width:200px"></div>
      <h6>Assignment Summary Per Associate Per Day</h6>
      <div class="table-wrap"><table class="data-table"><thead><tr>
        <th>Date</th><th>Associate</th><th>Shift Type</th>
        <th class="tc">Total Assigned</th><th class="tc">Completed</th><th class="tc">Released</th>
        <th class="tc">Within Limit Released</th><th class="tc">Beyond Limit Released</th>
        <th class="tc">Within-Limit Release %</th>
      </tr></thead><tbody id="asum-body"></tbody></table></div>
      <h6 class="mt-3">Released Ticket Details with Assignment Rank</h6>
      <div class="table-wrap"><table class="data-table"><thead><tr>
        <th>Ticket #</th><th>Associate</th><th>Shift</th>
        <th>Assigned At</th><th class="tc">Rank</th><th class="tc">Classification</th>
        <th>Released At</th><th>Validation Reason</th>
      </tr></thead><tbody id="asum-detail-body"></tbody></table></div>
    </div>
    <div class="tab-pane fade section-wrap" id="t5">
      <p class="text-muted small mb-1">Backlog charts show <strong>current state</strong> — not affected by date filter.</p>
      <div class="kpi-grid mb-3" style="grid-template-columns:repeat(4,1fr);padding:0">
        <div class="kpi-card"><div class="kpi-val" style="color:#dc3545">{kpi['open_tickets']}</div><div class="kpi-lbl">Open Now</div></div>
        <div class="kpi-card"><div class="kpi-val" id="bl-recv" style="color:#fd7e14">—</div><div class="kpi-lbl">Received in Range</div></div>
        <div class="kpi-card"><div class="kpi-val" id="bl-clos" style="color:#28a745">—</div><div class="kpi-lbl">Closed in Range</div></div>
        <div class="kpi-card"><div class="kpi-val" style="color:#6f42c1">{kpi['aged_backlog']}</div><div class="kpi-lbl">Aged Backlog</div></div>
      </div>
      <div class="row g-3">
        <div class="col-md-5"><div class="chart-card"><h6>Open Tickets by Age (Current)</h6><canvas id="bucketChart" height="220"></canvas></div></div>
        <div class="col-md-7"><div class="chart-card"><h6>Today — Inflow vs Closures by Hour (IST)</h6><canvas id="hourlyChart" height="220"></canvas></div></div>
      </div>
    </div>
    <div class="tab-pane fade section-wrap" id="t6">
      <div id="ac-summary" class="mb-2"></div>
      <div class="filter-row"><input type="text" id="f-ac" class="form-control form-control-sm" placeholder="Filter associate…" style="max-width:200px"></div>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Ticket #</th><th>Source</th><th>Assignee</th><th>Created</th><th class="tc">Age (h)</th><th>Reason</th></tr></thead><tbody id="ac-body"></tbody></table></div>
    </div>
    <div class="tab-pane fade section-wrap" id="t7">
      <div class="row g-3 mb-3">
        <div class="col-md-4"><div class="alert alert-danger mb-0 text-center"><div id="al-pend-n" style="font-size:2rem;font-weight:700">—</div>High Pending Associates</div></div>
        <div class="col-md-4"><div class="alert alert-warning mb-0 text-center"><div id="al-invrel-n" style="font-size:2rem;font-weight:700">—</div>Invalid Releases Today</div></div>
        <div class="col-md-4"><div class="alert alert-danger mb-0 text-center"><div id="al-sla-n" style="font-size:2rem;font-weight:700">—</div>Critical SLA Breaches</div></div>
      </div>
      <h6>High Pending</h6>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Associate</th><th>Shift</th><th class="tc">Pending</th><th class="tc">Threshold</th><th>Level</th></tr></thead><tbody id="al-pend-body"></tbody></table></div>
      <h6 class="mt-3">Invalid Releases Today</h6>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Associate</th><th class="tc">Count</th><th>Ticket IDs</th></tr></thead><tbody id="al-rel-body"></tbody></table></div>
      <h6 class="mt-3">SLA Breaches <small class="text-muted fw-normal">(Warn={SLA_WARN}h · Breach={SLA_BREACH}h · Critical={SLA_CRITICAL}h)</small></h6>
      <div id="sla-badges" class="mb-2"></div>
      <div class="table-wrap"><table class="data-table"><thead><tr><th>Ticket #</th><th>Assignee</th><th>Created</th><th class="tc">Age</th><th>SLA Status</th></tr></thead><tbody id="al-sla-body"></tbody></table></div>
    </div>
  </div>
</div>

<!-- ======================================================= DATA ======= -->
<script>
const DASH = {{
  releases:  {rel_js},
  quality:   {qual_js},
  perf:      {perf_js},
  asum:      {asum_js},
  ac:        {ac_js},
  sla:       {sla_js},
  daily_kpi: {dkpi_js},
  backlog:   {bl_js},
  h_inflow:  {h_inflow},
  h_closed:  {h_closed},
  pending:   {pending_js},
  ap:        {ap_js},
  live:      {live_kpi_js},
  quota:     {SPLIT_SHIFT_QUOTA},
  today:     '{today_iso}',
}};
</script>

<!-- ===================================================== RENDERER ===== -->
<script>
// ── State ──────────────────────────────────────────────────────────────────
let S = {{ start: '{week_ago}', end: '{today_iso}' }};
let bucketChart = null, hourlyChart = null;

// ── Helpers ────────────────────────────────────────────────────────────────
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const pct = (n,d) => d ? (n/d*100).toFixed(1) : '0.0';
const noRow = (cols, msg='No data in selected range') =>
  `<tr><td colspan="${{cols}}" class="no-data">${{msg}}</td></tr>`;
const filt = (arr, field='date') =>
  arr.filter(r => r[field] >= S.start && r[field] <= S.end);

function bar(p, color) {{
  const n = Math.min(parseFloat(p)||0, 100);
  const c = color || (n>=80?'#28a745':n>=50?'#ffc107':'#dc3545');
  return `<div class="bar-wrap"><div class="bar-fill" style="background:${{c}};width:${{n}}%"></div></div><small>${{p}}%</small>`;
}}
function stypeBadge(s) {{
  if (s==='Shift Lead') return '<span class="badge bg-info text-dark">Shift Lead</span>';
  if (s==='Split'||s==='split') return '<span class="badge bg-warning text-dark">Split</span>';
  return '<span class="badge bg-primary">Full</span>';
}}
function ageCls(h) {{ return h>=24?'age-crit':h>=8?'age-warn':''; }}
function hrsLabel(h) {{ return h<24?h.toFixed(1)+'h':(h/24).toFixed(1)+'d'; }}
function rankLabel(r) {{ return r>0?r:'—'; }}

// ── Quick date range setter ────────────────────────────────────────────────
function setQ(q) {{
  const d = new Date(); d.setHours(0,0,0,0);
  const fmt = x => x.toISOString().slice(0,10);
  if (q==='today')     {{ S.start=S.end=fmt(d); }}
  else if (q==='yesterday') {{ const y=new Date(d); y.setDate(y.getDate()-1); S.start=S.end=fmt(y); }}
  else if (q==='7d')   {{ const w=new Date(d); w.setDate(w.getDate()-6); S.start=fmt(w); S.end=fmt(d); }}
  else if (q==='30d')  {{ const m=new Date(d); m.setDate(m.getDate()-29); S.start=fmt(m); S.end=fmt(d); }}
  else if (q==='all')  {{ S.start='2020-01-01'; S.end=fmt(d); }}
  document.getElementById('startDate').value = S.start;
  document.getElementById('endDate').value   = S.end;
  document.querySelectorAll('.quick-btn').forEach(b => b.classList.remove('active'));
  const map = {{today:'today',yesterday:'yesterday','7d':'btn7d','30d':'btn30d'}};
  renderAll();
}}

// ── KPI section ────────────────────────────────────────────────────────────
function renderKPIs() {{
  const rows = filt(DASH.daily_kpi);
  const recv = rows.reduce((s,r)=>s+r.received,0);
  const clos = rows.reduce((s,r)=>s+r.closed,0);
  document.getElementById('kv-recv').textContent = recv.toLocaleString();
  document.getElementById('kv-clos').textContent = clos.toLocaleString();
  document.getElementById('kv-recv-sub').textContent = S.start===S.end?S.start:`${{S.start}} – ${{S.end}}`;
  document.getElementById('kv-clos-sub').textContent = S.start===S.end?S.start:`${{S.start}} – ${{S.end}}`;
  document.getElementById('bl-recv').textContent = recv.toLocaleString();
  document.getElementById('bl-clos').textContent = clos.toLocaleString();
  document.getElementById('rangeLabel').textContent = rows.length + ' days in range';
}}

// ── Performance section ────────────────────────────────────────────────────
function renderPerf() {{
  const rows = filt(DASH.perf);
  const filter_txt = (document.getElementById('f-perf').value||'').toLowerCase();
  const byAgent = {{}};
  rows.forEach(r => {{
    if (!byAgent[r.name]) byAgent[r.name] = {{name:r.name,stype:r.stype,auto:0,manual:0,io:0,closed:0}};
    byAgent[r.name].auto+=r.auto; byAgent[r.name].manual+=r.manual;
    byAgent[r.name].io+=r.io;     byAgent[r.name].closed+=r.closed;
  }});
  let sHtml = '';
  Object.values(byAgent)
    .filter(a => !filter_txt || a.name.toLowerCase().includes(filter_txt))
    .sort((a,b) => (b.auto+b.manual+b.io)-(a.auto+a.manual+a.io))
    .forEach(a => {{
      const total = a.auto+a.manual+a.io;
      const p = pct(a.closed,total);
      const pend = DASH.pending[a.name]||0;
      const pCls = pend>=(a.stype==='full'?20:10)?'text-danger fw-bold':'';
      sHtml += `<tr><td><strong>${{esc(a.name)}}</strong></td><td class="tc">${{stypeBadge(a.stype)}}</td>
        <td class="tc">${{total}}</td><td class="tc">${{a.auto}}</td><td class="tc">${{a.manual}}</td>
        <td class="tc">${{a.io}}</td><td class="tc">${{a.closed}}</td>
        <td class="tc ${{pCls}}">${{pend}}</td><td class="tc" style="min-width:120px">${{bar(p)}}</td></tr>`;
    }});
  document.getElementById('perf-sum-head').innerHTML = `<tr><th>Associate</th><th class="tc">Shift</th><th class="tc">Total</th><th class="tc">Auto</th><th class="tc">Manual</th><th class="tc">IO</th><th class="tc">Closed</th><th class="tc">Open Now</th><th class="tc">Closure %</th></tr>`;
  document.getElementById('perf-sum-body').innerHTML = sHtml || noRow(9);

  let dHtml = '';
  [...rows]
    .filter(r => !filter_txt || r.name.toLowerCase().includes(filter_txt))
    .sort((a,b) => b.date.localeCompare(a.date))
    .forEach(r => {{
      const total = r.auto+r.manual+r.io;
      dHtml += `<tr><td>${{esc(r.label||r.date)}}</td><td>${{esc(r.name)}}</td>
        <td class="tc">${{total}}</td><td class="tc">${{r.auto}}</td><td class="tc">${{r.manual}}</td>
        <td class="tc">${{r.io}}</td><td class="tc">${{r.closed}}</td>
        <td class="tc" style="min-width:120px">${{bar(pct(r.closed,total))}}</td></tr>`;
    }});
  document.getElementById('perf-day-head').innerHTML = `<tr><th>Date</th><th>Associate</th><th class="tc">Total</th><th class="tc">Auto</th><th class="tc">Manual</th><th class="tc">IO</th><th class="tc">Closed</th><th class="tc">Closure %</th></tr>`;
  document.getElementById('perf-day-body').innerHTML = dHtml || noRow(8);
}}

// ── Quality section ────────────────────────────────────────────────────────
function renderQuality() {{
  const rows = filt(DASH.quality);
  const filter_txt = (document.getElementById('f-qual').value||'').toLowerCase();
  const byAgent = {{}};
  rows.forEach(r => {{
    if (!byAgent[r.name]) byAgent[r.name]={{name:r.name,total:0,tagged:0,ops:0,resp:0,ok:0}};
    const a=byAgent[r.name]; a.total++; a.tagged+=r.tagged?1:0;
    a.ops+=r.ops?1:0; a.resp+=r.resp?1:0; a.ok+=r.closedok?1:0;
  }});
  let sHtml = '';
  Object.values(byAgent)
    .filter(a => !filter_txt || a.name.toLowerCase().includes(filter_txt))
    .sort((a,b) => {{
      const qa = (a.tagged+a.ops+a.resp+a.ok)/(a.total*4||1);
      const qb = (b.tagged+b.ops+b.resp+b.ok)/(b.total*4||1);
      return qa-qb;
    }})
    .forEach(a => {{
      const overall = pct(a.tagged+a.ops+a.resp+a.ok, a.total*4);
      const c = parseFloat(overall)>=80?'#28a745':parseFloat(overall)>=60?'#ffc107':'#dc3545';
      sHtml += `<tr><td><strong>${{esc(a.name)}}</strong></td><td class="tc">${{a.total}}</td>
        <td class="tc" style="min-width:110px">${{bar(pct(a.tagged,a.total),'#6f42c1')}}</td>
        <td class="tc" style="min-width:110px">${{bar(pct(a.ops,a.total),'#20c997')}}</td>
        <td class="tc" style="min-width:110px">${{bar(pct(a.resp,a.total),'#fd7e14')}}</td>
        <td class="tc" style="min-width:110px">${{bar(pct(a.ok,a.total),'#0dcaf0')}}</td>
        <td class="tc" style="min-width:110px">${{bar(overall,c)}}</td></tr>`;
    }});
  document.getElementById('qual-sum-head').innerHTML = `<tr><th>Associate</th><th class="tc">Tickets</th><th class="tc">Tagged %</th><th class="tc">Ops Mapped %</th><th class="tc">Responded %</th><th class="tc">Correct Closure %</th><th class="tc">Overall Quality %</th></tr>`;
  document.getElementById('qual-sum-body').innerHTML = sHtml || noRow(7);

  const failing = rows.filter(r => r.score < 4 && (!filter_txt || r.name.toLowerCase().includes(filter_txt)));
  document.getElementById('qual-fail-count').textContent = `(${{failing.length}} tickets)`;
  let fHtml = '';
  failing.sort((a,b)=>a.score-b.score).forEach(r => {{
    const flags = [];
    if (!r.tagged)    flags.push('<span class="badge bg-danger">No Tag</span>');
    if (!r.ops)       flags.push('<span class="badge bg-warning text-dark">No Ops</span>');
    if (!r.resp)      flags.push('<span class="badge bg-secondary">No Response</span>');
    if (!r.closedok)  flags.push('<span class="badge bg-dark">Bad Closure</span>');
    fHtml += `<tr><td>#${{r.tid}}</td><td>${{esc(r.name)}}</td><td>${{esc(r.created)}}</td>
      <td>${{flags.join(' ')}}</td><td class="tc">${{r.score}}/4</td></tr>`;
  }});
  document.getElementById('qual-fail-head').innerHTML = `<tr><th>Ticket #</th><th>Assignee</th><th>Created</th><th>Failing Checks</th><th class="tc">Score</th></tr>`;
  document.getElementById('qual-fail-body').innerHTML = fHtml || noRow(5,'All tickets pass quality checks ✅');
}}

// ── Releases section ───────────────────────────────────────────────────────
function renderReleases() {{
  const rows = filt(DASH.releases);
  const filter_txt = (document.getElementById('f-rel').value||'').toLowerCase();
  const inv   = rows.filter(r=>r.inv);
  const valid = rows.filter(r=>!r.inv);
  const sl    = rows.filter(r=>r.rule==='shift_lead');

  document.getElementById('rv-total').textContent = rows.length;
  document.getElementById('rv-inv').textContent   = inv.length;
  document.getElementById('rv-val').textContent   = valid.length;
  document.getElementById('rv-sl').textContent    = sl.length;
  document.getElementById('rv-inv-pct').textContent = pct(inv.length,rows.length)+'%';
  document.getElementById('badge-inv').textContent = inv.length || '';
  document.getElementById('rel-inv-count').textContent = `(${{inv.length}})`;
  document.getElementById('rel-val-count').textContent = `(${{valid.length}})`;

  // Summary per agent
  const byAgent = {{}};
  rows.filter(r => !filter_txt || r.name.toLowerCase().includes(filter_txt)).forEach(r => {{
    if (!byAgent[r.name]) byAgent[r.name]={{name:r.name,stype:r.stype,total:0,inv:0,val:0,dtotal:r.dtotal,inv_tids:[]}};
    const a=byAgent[r.name]; a.total++;
    if(r.inv){{a.inv++;a.inv_tids.push(r.tid);}} else a.val++;
  }});
  let sumHtml = '';
  Object.values(byAgent).sort((a,b)=>b.inv-a.inv).forEach(a => {{
    const tids = a.inv_tids.slice(0,8).map(t=>`#${{t}}`).join(', ')+(a.inv_tids.length>8?'…':'');
    sumHtml += `<tr><td><strong>${{esc(a.name)}}</strong></td><td>${{stypeBadge(a.stype)}}</td>
      <td class="tc">${{a.total}}</td>
      <td class="tc ${{a.inv?'text-danger fw-bold':''}}">${{a.inv||'—'}}</td>
      <td class="tc ${{a.val?'text-success':''}}">${{a.val||'—'}}</td>
      <td class="tc">${{a.dtotal}}</td>
      <td><small>${{esc(tids)}}</small></td></tr>`;
  }});
  document.getElementById('rel-sum-body').innerHTML = sumHtml || noRow(7,'No releases in selected range');

  const relRow = (r) => {{
    const bc = r.rule==='shift_lead' ? 'bg-info text-dark'
             : r.flag.startsWith('Unknown') ? 'bg-secondary'
             : r.inv ? 'bg-danger'
             : 'bg-success';
    return `<tr><td>#${{r.tid}}</td><td>${{esc(r.name)}}</td><td>${{stypeBadge(r.stype)}}</td>
      <td>${{esc(r.assigned)}}</td><td>${{esc(r.released)}}</td>
      <td class="tc">${{r.dtotal}}</td><td class="tc">${{rankLabel(r.rank)}}</td>
      <td class="tc">${{esc(r.vfrom)}}</td><td class="tc">${{esc(r.shend)}}</td>
      <td><span class="badge ${{bc}} tw" style="max-width:260px">${{esc(r.flag)}}</span></td></tr>`;
  }};

  const filtInv = inv.filter(r=>!filter_txt||r.name.toLowerCase().includes(filter_txt))
                     .sort((a,b)=>b.released.localeCompare(a.released));
  document.getElementById('rel-inv-body').innerHTML = filtInv.map(relRow).join('') || noRow(10,'No invalid releases ✅');

  const filtVal = valid.filter(r=>!filter_txt||r.name.toLowerCase().includes(filter_txt))
                       .sort((a,b)=>b.released.localeCompare(a.released));
  document.getElementById('rel-val-body').innerHTML = filtVal.map(relRow).join('') || noRow(10,'No valid releases');
}}

// ── Assignment Summary section ─────────────────────────────────────────────
function renderAssignSummary() {{
  const rows = filt(DASH.asum);
  const filter_txt = (document.getElementById('f-asum').value||'').toLowerCase();
  const quota = DASH.quota;

  let sHtml = '';
  rows.filter(r => !filter_txt || r.name.toLowerCase().includes(filter_txt))
      .forEach(r => {{
    const wipPct = pct(r.within_released, r.released||1);
    const wipCls = parseFloat(wipPct)>0?'text-danger fw-bold':'text-success';
    sHtml += `<tr>
      <td>${{esc(r.date)}}</td><td>${{esc(r.name)}}</td><td>${{stypeBadge(r.stype)}}</td>
      <td class="tc"><strong>${{r.total}}</strong></td>
      <td class="tc">${{r.completed}}</td>
      <td class="tc">${{r.released}}</td>
      <td class="tc ${{wipCls}}">${{r.within_released}}</td>
      <td class="tc">${{r.beyond_released}}</td>
      <td class="tc" style="min-width:110px">${{bar(wipPct,'#dc3545')}}</td>
    </tr>`;
  }});
  document.getElementById('asum-body').innerHTML = sHtml || noRow(9);

  // Released ticket details with rank
  const relRows = filt(DASH.releases).filter(r =>
    (r.stype==='Split'||r.stype==='split') &&
    (!filter_txt||r.name.toLowerCase().includes(filter_txt))
  ).sort((a,b)=>b.released.localeCompare(a.released));

  let dHtml = '';
  relRows.forEach(r => {{
    const inLimit = r.rank>0 && r.rank<=quota;
    const cls = inLimit?'badge bg-danger':'badge bg-success';
    const lbl = inLimit?'Within Limit (should complete)':'Beyond Limit (may release)';
    const vc  = r.inv?'badge bg-danger':'badge bg-success';
    dHtml += `<tr>
      <td>#${{r.tid}}</td><td>${{esc(r.name)}}</td><td>${{stypeBadge(r.stype)}}</td>
      <td>${{esc(r.assigned)}}</td>
      <td class="tc"><strong>${{rankLabel(r.rank)}}</strong></td>
      <td><span class="${{cls}}">${{lbl}}</span></td>
      <td>${{esc(r.released)}}</td>
      <td><span class="${{vc}}">${{esc(r.flag.length>40?r.flag.slice(0,40)+'…':r.flag)}}</span></td>
    </tr>`;
  }});
  document.getElementById('asum-detail-body').innerHTML = dHtml || noRow(8,'No split-shift releases in range');
}}

// ── Backlog section (charts — current state, not date-filtered) ────────────
function initCharts() {{
  const bLabels = DASH.backlog.map(b=>b.bucket);
  const bData   = DASH.backlog.map(b=>b.cnt);
  bucketChart = new Chart(document.getElementById('bucketChart'),{{
    type:'doughnut',
    data:{{labels:bLabels,datasets:[{{data:bData,backgroundColor:['#28a745','#ffc107','#fd7e14','#dc3545','#6f42c1'],borderWidth:1}}]}},
    options:{{plugins:{{legend:{{position:'bottom'}}}},cutout:'55%'}}
  }});
  const hLabels = Array.from({{length:24}},(_,i)=>String(i).padStart(2,'0')+':00');
  hourlyChart = new Chart(document.getElementById('hourlyChart'),{{
    type:'bar',
    data:{{labels:hLabels,datasets:[
      {{label:'Received',data:DASH.h_inflow,backgroundColor:'rgba(253,126,20,.7)'}},
      {{label:'Closed',  data:DASH.h_closed,backgroundColor:'rgba(40,167,69,.7)'}}
    ]}},
    options:{{responsive:true,scales:{{y:{{beginAtZero:true}}}},plugins:{{legend:{{position:'top'}}}}}}
  }});
}}

// ── Auto-closure section ───────────────────────────────────────────────────
function renderAutoClose() {{
  const rows = filt(DASH.ac);
  const filter_txt = (document.getElementById('f-ac').value||'').toLowerCase();
  document.getElementById('badge-ac').textContent = rows.length||'';
  const byReason = {{}};
  rows.forEach(r => {{const k=r.reason.split(':')[0].split('—')[0].trim(); byReason[k]=(byReason[k]||0)+1;}});
  document.getElementById('ac-summary').innerHTML =
    `<div class="alert alert-info py-2 small"><strong>${{rows.length}}</strong> tickets eligible for auto-closure &nbsp;|&nbsp;`+
    Object.entries(byReason).map(([k,v])=>`<span class="badge bg-secondary me-1">${{esc(k)}}: ${{v}}</span>`).join('')+'</div>';
  let html = '';
  rows.filter(r=>!filter_txt||r.name.toLowerCase().includes(filter_txt))
      .forEach(r => {{
    const aCls = ageCls(r.hrs);
    const src = (r.source||'').toLowerCase().replace(/[^a-z]/g,'');
    html += `<tr><td>#${{r.tid}}</td>
      <td><span class="badge-src src-${{src}}">${{esc(r.source)}}</span></td>
      <td>${{esc(r.name)}}</td><td>${{esc(r.created)}}</td>
      <td class="tc ${{aCls}}">${{hrsLabel(r.hrs)}}</td>
      <td><small>${{esc(r.reason)}}</small></td></tr>`;
  }});
  document.getElementById('ac-body').innerHTML = html || noRow(6,'No auto-closure suggestions in range ✅');
}}

// ── Alerts section ─────────────────────────────────────────────────────────
function renderAlerts() {{
  // High pending (current state — not date-filtered)
  const ap = DASH.ap;
  document.getElementById('al-pend-n').textContent = ap.length;
  let pHtml = '';
  ap.forEach(a => {{
    const lvlCls = a.level==='HIGH'?'table-danger':'table-warning';
    const bc = a.level==='HIGH'?'bg-danger':'bg-warning text-dark';
    pHtml += `<tr class="${{lvlCls}}"><td><strong>${{esc(a.name)}}</strong></td>
      <td class="tc">${{esc(a.shift)}}</td><td class="tc text-danger fw-bold">${{a.pending}}</td>
      <td class="tc">${{a.threshold}}</td>
      <td><span class="badge ${{bc}}">${{a.level}}</span></td></tr>`;
  }});
  document.getElementById('al-pend-body').innerHTML = pHtml || noRow(5,'No high-pending alerts ✅');

  // Invalid releases today (from date-filtered releases)
  const today = DASH.today;
  const todayInv = DASH.releases.filter(r=>r.inv && r.date===today);
  const invByAgent = {{}};
  todayInv.forEach(r=>{{if(!invByAgent[r.name]) invByAgent[r.name]=[];invByAgent[r.name].push(r.tid);}});
  const invAgents = Object.entries(invByAgent).sort((a,b)=>b[1].length-a[1].length);
  document.getElementById('al-invrel-n').textContent = invAgents.length;
  let rHtml = '';
  invAgents.forEach(([name,tids])=>{{
    rHtml += `<tr class="table-danger"><td><strong>${{esc(name)}}</strong></td>
      <td class="tc text-danger fw-bold">${{tids.length}}</td>
      <td><small>${{tids.slice(0,8).map(t=>'#'+t).join(', ')}}${{tids.length>8?'…':''}}</small></td></tr>`;
  }});
  document.getElementById('al-rel-body').innerHTML = rHtml || noRow(3,'No invalid releases today ✅');

  // SLA breaches (date-filtered by ticket created date)
  const sla = filt(DASH.sla);
  const crit = sla.filter(r=>r.level==='CRITICAL').length;
  const brch = sla.filter(r=>r.level==='BREACH').length;
  const warn = sla.filter(r=>r.level==='WARNING').length;
  document.getElementById('al-sla-n').textContent = crit;
  document.getElementById('badge-al').textContent = (ap.length+crit)||'';
  document.getElementById('sla-badges').innerHTML =
    `<span class="badge bg-info text-dark me-1">Warning: ${{warn}}</span>`+
    `<span class="badge bg-warning text-dark me-1">Breach: ${{brch}}</span>`+
    `<span class="badge bg-danger me-1">Critical: ${{crit}}</span>`;
  let sHtml = '';
  sla.sort((a,b)=>b.hrs-a.hrs).forEach(r=>{{
    const rc = r.level==='CRITICAL'?'table-danger':r.level==='BREACH'?'table-warning':'';
    const bc = r.level==='CRITICAL'?'bg-danger':r.level==='BREACH'?'bg-warning text-dark':'bg-info text-dark';
    sHtml += `<tr class="${{rc}}"><td>#${{r.tid}}</td><td>${{esc(r.name)}}</td>
      <td>${{esc(r.created)}}</td>
      <td class="tc ${{ageCls(r.hrs)}}">${{hrsLabel(r.hrs)}}</td>
      <td><span class="badge ${{bc}}">${{r.level}}</span></td></tr>`;
  }});
  document.getElementById('al-sla-body').innerHTML = sHtml || noRow(5,'No SLA breaches ✅');
}}

// ── Master render ──────────────────────────────────────────────────────────
function renderAll() {{
  const errEl = document.getElementById('dateErr');
  if (S.start > S.end) {{
    errEl.textContent = '⚠ Start date cannot be after end date';
    return;
  }}
  errEl.textContent = '';
  renderKPIs();
  renderPerf();
  renderQuality();
  renderReleases();
  renderAssignSummary();
  renderAutoClose();
  renderAlerts();
}}

// ── Event listeners ────────────────────────────────────────────────────────
document.getElementById('startDate').addEventListener('change', e => {{ S.start=e.target.value; renderAll(); }});
document.getElementById('endDate').addEventListener('change',   e => {{ S.end=e.target.value;   renderAll(); }});
document.getElementById('f-perf').addEventListener('input', renderPerf);
document.getElementById('f-qual').addEventListener('input', renderQuality);
document.getElementById('f-rel').addEventListener('input',  renderReleases);
document.getElementById('f-asum').addEventListener('input', renderAssignSummary);
document.getElementById('f-ac').addEventListener('input',   renderAutoClose);

// ── Boot ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {{
  initCharts();
  renderAll();
}});
</script>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
{auto_reload}
</body>
</html>"""


def generate(db_url, refresh_secs=0):
    roster = load_roster()
    print('Connecting to DB…', flush=True)
    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        print(f'ERROR connecting to DB: {e}'); return False

    try:
        print('  Fetching data…', flush=True)
        data = fetch_all(conn, roster)
        kpi  = data['kpi']
        print(f"  Open: {kpi['open_tickets']}  Today inflow: {kpi['received_today']}"
              f"  Closed: {kpi['closed_today']}  Aged backlog: {kpi['aged_backlog']}", flush=True)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f'ERROR fetching data: {e}'); conn.close(); return False
    finally:
        conn.close()

    print('  Rendering HTML…', flush=True)
    html = build_html(data, refresh_secs)
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Saved → {OUT_FILE}', flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description='Cityflo CS Monitoring Dashboard')
    parser.add_argument('--watch', nargs='?', const=5, type=int, metavar='MINUTES',
                        help='Auto-refresh interval in minutes (default 5)')
    args = parser.parse_args()

    env     = load_env()
    db_url  = env.get('DATABASE_URL')
    if not db_url:
        print('ERROR: DATABASE_URL not found in env file.'); return

    refresh_secs = args.watch * 60 if args.watch else 0
    ok = generate(db_url, refresh_secs)
    if ok:
        webbrowser.open(f'file:///{OUT_FILE.replace(chr(92), "/")}')

    if args.watch:
        interval = args.watch * 60
        print(f'\n  Auto-refresh every {args.watch} min. Press Ctrl+C to stop.\n', flush=True)
        try:
            while True:
                time.sleep(interval)
                now = datetime.now(IST).strftime('%H:%M:%S')
                print(f'\n[{now}] Refreshing…', flush=True)
                generate(db_url, refresh_secs)
        except KeyboardInterrupt:
            print('\nStopped.')


if __name__ == '__main__':
    main()
