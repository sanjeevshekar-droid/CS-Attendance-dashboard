# -*- coding: utf-8 -*-
"""
Cityflo CS Dashboard  (merged Ticket Queue + Agent Audit)
==========================================================
Generates a single HTML file with two top-level tabs:

  TAB 1 — Ticket Queue
    Priority-sorted live view of all open tickets.
    Priorities: CLOSE NOW | CX WAITING | NEW/NO REPLY | PENDING REPLY | NEEDS REVIEW | IO

  TAB 2 — Agent Audit  (last 7 days)
    1. Daily Tickets Assigned   (auto + manual + IO self-raised)
    2. Pre-Logoff Releases       (tickets closed within 1 hr before shift end)
    3. Responded & Closed        (tickets the agent both replied to AND resolved)
    4. Responded, Not Highlighted (open tickets: agent replied but no Ops Assignee set)

Usage:
    python ticket_dashboard.py              # generate once and open browser
    python ticket_dashboard.py --watch      # auto-refresh every 5 minutes
    python ticket_dashboard.py --watch 3    # custom interval (minutes)
"""
import sys, io, json, os, re, webbrowser, argparse, time
import psycopg2
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta, date, time as dtime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_URL      = ('postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster'
               '.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/'
               'cityflo_final_backend?sslmode=prefer')
IST         = timezone(timedelta(hours=5, minutes=30))
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUT_FILE    = os.path.join(BASE_DIR, 'ticket_dashboard.html')
ROSTER_FILE = os.path.join(BASE_DIR, 'roster.json')
AUDIT_DAYS  = 7   # how many past days the Agent Audit covers


# ════════════════════════════════════════════════════════════════════════════
# Shared constants
# ════════════════════════════════════════════════════════════════════════════

AGENT_PHRASES = [
    'good morning','good afternoon','good evening','we apologize',
    'inconvenience','highlighted the issue','relevant team','allow us',
    'sorry to hear','sincerely apologize','i have escalated','we will check',
    'we will look into','we take this seriously','please allow','kindly allow',
    'we will get back','thank you for','thanks for reaching','we have noted',
    'rest assured','we will resolve','i hope this helps',
    'we kindly request you to please elaborate','kindly elaborate','please elaborate',
    'we have shared','we have highlighted','i have highlighted','i have addressed',
    'we will update you','we will inform','sorry for the inconvenience',
    'we apologize for the inconvenience','i am sorry to hear','we have escalated',
]
SAGE_NOISE = [
    'cityflo support assistant','please choose an option','hey there',
    'what can i assist','main menu','choose from below',
    'how can i help','select an option',
]
SRC_LABEL = {'1': 'App', '7': 'Social', '8': 'IO/WA', '9': 'Sage'}


# ════════════════════════════════════════════════════════════════════════════
# Shared text helpers
# ════════════════════════════════════════════════════════════════════════════

def get_yellow_text(text):
    try:
        data = json.loads(text)
        msgs = []
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    msgs.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)): walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj: walk(item, in_yellow)
        walk(data)
        return ' '.join(msgs)
    except Exception:
        return ''


def is_agent_reply(comment):
    if not comment: return False
    try:
        json.loads(comment); return False
    except Exception:
        return any(p in comment.lower() for p in AGENT_PHRASES)


def is_customer_msg(comment):
    if not comment: return False
    try:
        data = json.loads(comment)
        msgs = []
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    msgs.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)): walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj: walk(item, in_yellow)
        walk(data)
        return len(' '.join(msgs).strip()) > 5
    except Exception:
        return not is_agent_reply(comment) and len(comment.strip()) > 8


def get_customer_text(comments):
    msgs = []
    for comment, is_internal, _ in comments:
        if is_internal: continue
        yellow = get_yellow_text(comment)
        if yellow:
            y = yellow.lower()
            if not any(n in y for n in SAGE_NOISE):
                msgs.append(yellow)
    return ' '.join(msgs).lower()


def _esc(s):
    return (str(s)
            .replace('&','&amp;').replace('<','&lt;')
            .replace('>','&gt;').replace('"','&quot;'))


# ════════════════════════════════════════════════════════════════════════════
# Roster helpers  (for Agent Audit – pre-logoff detection)
# ════════════════════════════════════════════════════════════════════════════

def load_roster():
    with open(ROSTER_FILE, encoding='utf-8') as f:
        return json.load(f)


def _parse_t(s):
    h, m = map(int, s.split(':'))
    return dtime(h, m)


def _norm_name(s):
    return re.sub(r'\s+', ' ', (s or '').lower().strip())


def get_last_shift_end(roster, name_db, target_date):
    target_str = target_date.strftime('%Y-%m-%d')
    entry = None
    for wr in roster.get('weekend_rosters', []):
        if wr.get('date') == target_str:
            for a in wr.get('associates', []):
                if _norm_name(name_db) in _norm_name(a.get('name_db', '')):
                    entry = a; break
            if entry: break
    if not entry:
        for a in roster.get('associates', []):
            if _norm_name(name_db) in _norm_name(a.get('name_db', '')):
                entry = a; break
    if not entry: return None
    ends = []
    for slot_key in ('morning', 'evening'):
        slot = entry.get(slot_key)
        if slot:
            ends.append(datetime.combine(target_date, _parse_t(slot['end']), tzinfo=IST))
    return max(ends) if ends else None


# ════════════════════════════════════════════════════════════════════════════
# Ticket classification  (Tab 1)
# ════════════════════════════════════════════════════════════════════════════

def label_ticket(cust):
    c = cust.lower()
    if any(k in c for k in [
            'i lost or found','earphone','earpod','earbuds','airpod',
            'left my bottle','forgot my bottle','left my bag','forgot my bag',
            'lost my bag','left my phone','forgot my phone','lost my phone',
            'lost my wallet','forgot my wallet','left my charger','left my keys',
            'forgot my keys','left my glasses','left my umbrella','lost one item',
            'pouch on seat','tiffin','lunch box','lunchbox',
            'forgot on seat','left on bus','left in bus','forgot on bus']):
        return 'LOST_FOUND','Lost & Found','MANUAL'
    if any(k in c for k in [
            'issue with driver','driving rashly','rash driving','behaving rudely',
            'rude','wrong route','took other route','unscheduled stop',
            'talking on phone','talking on call','honking',
            'driving very slowly','driving slow','other driver issue',
            "didn't stop at designated",'misbehaved','unprofessional driver']):
        return 'DRIVER','Driver Behaviour','MANUAL'
    if any(k in c for k in [
            'issue with ac','ac is not working','ac was not working','ac not working',
            'increase the ac','decrease the ac','ac was very cold','ac was very hot',
            'ac vent is broken','ac vent was broken','ac is making noise',
            'ac was making noise','no ac','ac off','no cooling','ac problem']):
        return 'AC','AC Issue','AUTO-CLOSE'
    if any(k in c for k in [
            'my seat has a problem','my seat had a problem',
            'slider is not working','slider was not working',
            'handrest is broken','handrest was broken',
            'recliner is not working','recliner was not working',
            'footrest is broken','footrest was broken',
            'charging point is not working','charging point was not working',
            'bottle holder is broken','seat pocket is broken']):
        return 'SEAT','Seat / Hardware','AUTO-CLOSE'
    if any(k in c for k in [
            'bus quality and hygiene','bus was not clean','bus is not clean',
            'water is dripping','water was dripping','bus making noise',
            'bus was making noise','flies in the bus','bus broke down',
            'bus is in poor condition','bus was in poor condition']):
        return 'HYGIENE','Bus Hygiene','AUTO-CLOSE'
    if any(k in c for k in [
            'where is my bus',"i can't track the bus",
            "i couldn't track the bus",'tracking is wrong',
            'tracking was wrong','the bus is not moving','the bus was not moving']):
        return 'TRACKING','Tracking Issue','AUTO-CLOSE'
    if any(k in c for k in [
            'i want to reschedule','i missed my bus','i want a later bus',
            'i want an earlier bus','i want to change pickup stop',
            'i want to cancel this ride']):
        return 'RESCHEDULE','Reschedule / Cancel','REVIEW'
    if any(k in c for k in [
            'the bus was late','the bus is late','the bus left early',
            'the bus did not wait',"the bus didn't wait",'bus left before time']):
        return 'BUS_TIMING','Bus Timing','REVIEW'
    if any(k in c for k in [
            'payment related issue','i want refund','amount deducted',
            'amount not refunded','double charge','charged twice',
            'i want payment invoice','paid multiple times']):
        return 'PAYMENT','Payment / Refund','REVIEW'
    if any(k in c for k in [
            'app issue','app not working','unable to book',
            'unable to cancel','unable to reschedule','cannot book','login issue']):
        return 'APP','App / Booking','REVIEW'
    if any(k in c for k in [
            'suggestions: route, timing, stop','existing route',
            'subscription','referral','suggestions','b2b','rentals','other']):
        return 'SUGGESTION','Suggestion','AUTO-CLOSE'
    if len(c.strip()) < 10:
        return 'MENU_ONLY','No Issue Stated','AUTO-CLOSE'
    return 'UNKNOWN','Other / Unclear','REVIEW'


def _age_group(h):
    if h < 2:  return '< 2h'
    if h < 6:  return '2–6h'
    if h < 24: return '6–24h'
    if h < 72: return '1–3d'
    return '3d+'


# ════════════════════════════════════════════════════════════════════════════
# DB fetch — Tab 1: Ticket Queue
# ════════════════════════════════════════════════════════════════════════════

def fetch_ticket_data(conn):
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    cur     = conn.cursor()

    cur.execute("""
        SELECT t.id, t.source, t.info_tag, t.created, t.sageai_category_slug,
               COALESCE(p.first_name || ' ' || p.last_name, e.email, 'Unassigned') AS assignee
        FROM support_ticket t
        LEFT JOIN users_employee e ON e.id = t.assigned_to_employee_id
        LEFT JOIN users_person   p ON p.id = e.person_id
        WHERE t.status = '1'
          AND t.created >= NOW() - INTERVAL '60 days'
        ORDER BY t.id DESC
    """)
    ticket_rows = cur.fetchall()
    ticket_ids  = [r[0] for r in ticket_rows]

    if not ticket_ids:
        cur.close()
        return [], [], now_utc, now_ist

    ticket_meta = {}
    for tid, source, info_tag, created, sage_cat, assignee in ticket_rows:
        ticket_meta[tid] = {
            'source':   str(source or ''),
            'info_tag': info_tag or '',
            'created':  created,
            'sage_cat': sage_cat or '',
            'assignee': (assignee or 'Unassigned').strip(),
        }

    ph = ','.join(['%s'] * len(ticket_ids))
    cur.execute(f"""
        SELECT ticket_id, comment, is_internal, created
        FROM support_ticketcomment
        WHERE ticket_id IN ({ph})
        ORDER BY ticket_id, created ASC
    """, ticket_ids)

    raw_comments = defaultdict(list)
    for tid, comment, is_internal, tc_created in cur.fetchall():
        raw_comments[tid].append((comment or '', bool(is_internal), tc_created))

    # Not tagged: open tickets with at least one public comment but no tag_by_cityflo_id
    cur.execute("""
        SELECT t.id, t.created, t.source,
               COALESCE(p.first_name || ' ' || p.last_name, 'Unassigned') AS assignee
        FROM support_ticket t
        LEFT JOIN users_employee e ON e.id = t.assigned_to_employee_id
        LEFT JOIN users_person   p ON p.id = e.person_id
        WHERE t.status = '1'
          AND t.tag_by_cityflo_id IS NULL
          AND t.created >= NOW() - INTERVAL '60 days'
          AND EXISTS (
              SELECT 1 FROM support_ticketcomment tc2
              WHERE tc2.ticket_id = t.id AND tc2.is_internal = FALSE
          )
        ORDER BY t.created ASC
    """)
    not_tagged = []
    for nt_id, nt_created, nt_source, nt_assignee in cur.fetchall():
        ca = nt_created.replace(tzinfo=timezone.utc) if nt_created.tzinfo is None else nt_created
        hrs = round((now_utc - ca).total_seconds() / 3600, 1)
        not_tagged.append({
            'id':          nt_id,
            'created_str': ca.astimezone(IST).strftime('%d %b %Y %I:%M %p'),
            'source':      str(nt_source or ''),
            'src_label':   SRC_LABEL.get(str(nt_source or ''), str(nt_source or '')),
            'assignee':    (nt_assignee or 'Unassigned').strip(),
            'hrs_open':    hrs,
            'date_iso':    ca.astimezone(IST).strftime('%Y-%m-%d'),
        })

    cur.close()

    AUTO_CLOSE_CATS = {'AC','SEAT','HYGIENE','TRACKING','SUGGESTION','MENU_ONLY'}
    tickets = []

    for tid in ticket_ids:
        meta     = ticket_meta[tid]
        source   = meta['source']
        assignee = meta['assignee']
        created  = meta['created']
        comments = raw_comments.get(tid, [])

        created_aware = created.replace(tzinfo=timezone.utc) if created.tzinfo is None else created
        hours_open    = round((now_utc - created_aware).total_seconds() / 3600, 1)
        created_str   = created_aware.astimezone(IST).strftime('%d %b %Y %I:%M %p')
        src_label     = SRC_LABEL.get(source, source)
        public_cmt    = [(c,ii,t) for c,ii,t in comments if not ii]

        if source == '8':
            last_msg = ''
            for c, ii, _ in reversed(comments):
                if not ii and c.strip(): last_msg = c.strip()[:80]; break
            tickets.append({
                'id': tid, 'source': source, 'src_label': src_label,
                'category': 'IO', 'cat_label': 'IO / WhatsApp',
                'assignee': assignee, 'hours_open': hours_open,
                'created_str': created_str, 'priority': 'IO',
                'close_reason': '', 'last_msg': last_msg,
                'last_agent_hrs': None, 'last_cx_hrs': None,
                'age_group': _age_group(hours_open),
                'date_iso': created_aware.astimezone(IST).strftime('%Y-%m-%d'),
            })
            continue

        last_agent_time = last_cx_time = None
        last_msg = ''

        for comment, is_internal, tc_created in public_cmt:
            if is_agent_reply(comment):
                if last_agent_time is None or tc_created > last_agent_time:
                    last_agent_time = tc_created
            elif is_customer_msg(comment):
                if last_cx_time is None or tc_created > last_cx_time:
                    last_cx_time = tc_created

        for comment, is_internal, _ in reversed(public_cmt):
            if comment.strip():
                try:
                    yellow = get_yellow_text(comment)
                    last_msg = yellow[:80] if yellow else comment.strip()[:80]
                except Exception:
                    last_msg = comment.strip()[:80]
                break

        def aware(t):
            return t if t and t.tzinfo else (t.replace(tzinfo=timezone.utc) if t else None)

        last_agent_hrs = round((now_utc - aware(last_agent_time)).total_seconds()/3600,1) \
                         if last_agent_time else None
        last_cx_hrs    = round((now_utc - aware(last_cx_time)).total_seconds()/3600,1) \
                         if last_cx_time else None

        cust_text = get_customer_text(comments)
        cat_key, cat_label, disposition = label_ticket(cust_text)

        priority = 'REVIEW'; close_reason = ''

        if not public_cmt and source == '1':
            priority = 'CLOSE_NOW'; close_reason = 'EMPTY'
            cat_key = 'EMPTY'; cat_label = 'Empty / No Issue'
        elif cat_key in AUTO_CLOSE_CATS and source != '7':
            has_real_plain = False
            for c, ii, _ in public_cmt:
                if ii: continue
                try: json.loads(c)
                except Exception:
                    if not is_agent_reply(c) and len(c.strip()) > 8:
                        has_real_plain = True; break
            if not has_real_plain:
                priority = 'CLOSE_NOW'; close_reason = cat_key
        elif (last_agent_hrs is not None and
              (last_cx_time is None or aware(last_agent_time) > aware(last_cx_time)) and
              last_agent_hrs >= 24):
            priority = 'CLOSE_NOW'; close_reason = 'SILENT_24H'
        elif last_cx_time and (last_agent_time is None or
             aware(last_cx_time) > aware(last_agent_time)):
            priority = 'CX_WAITING'
        elif last_agent_time is None and public_cmt:
            priority = 'NEW_OPEN'
        elif last_agent_hrs is not None and last_agent_hrs < 24:
            priority = 'PENDING_CX'
        elif not public_cmt:
            priority = 'CLOSE_NOW' if disposition == 'AUTO-CLOSE' else 'NEW_OPEN'
            if disposition == 'AUTO-CLOSE': close_reason = cat_key
        elif disposition in ('REVIEW','MANUAL'):
            priority = 'REVIEW'

        tickets.append({
            'id': tid, 'source': source, 'src_label': src_label,
            'category': cat_key, 'cat_label': cat_label,
            'assignee': assignee, 'hours_open': hours_open,
            'created_str': created_str, 'priority': priority,
            'close_reason': close_reason, 'last_msg': last_msg,
            'last_agent_hrs': last_agent_hrs, 'last_cx_hrs': last_cx_hrs,
            'age_group': _age_group(hours_open),
            'date_iso': created_aware.astimezone(IST).strftime('%Y-%m-%d'),
        })

    return tickets, not_tagged, now_utc, now_ist


# ════════════════════════════════════════════════════════════════════════════
# DB fetch — Tab 2: Agent Audit
# ════════════════════════════════════════════════════════════════════════════

def fetch_audit_data(conn, roster):
    now_utc   = datetime.now(timezone.utc)
    now_ist   = now_utc.astimezone(IST)
    since_utc = now_utc - timedelta(days=AUDIT_DAYS + 1)
    cur       = conn.cursor()

    cur.execute("""
        SELECT DISTINCT e.id, p.first_name, p.last_name, p.user_id, p.id as person_id
        FROM support_ticketchange tc
        JOIN users_employee e ON e.id = tc.author_id
        JOIN users_person   p ON p.id = e.person_id
        WHERE tc.created >= %s AND tc.field = 'Status'
    """, (since_utc,))
    employees = {}
    for emp_id, first, last, user_id, person_id in cur.fetchall():
        employees[emp_id] = {
            'name': f"{first or ''} {last or ''}".strip(),
            'user_id': user_id, 'person_id': person_id,
        }

    if not employees:
        cur.close()
        return {'employees':{},'daily_assign':{},'pre_logoff':[],
                'resp_closed':{},'not_highlighted':[]}, now_utc, now_ist

    emp_ids  = list(employees.keys())
    user_ids = [str(v['user_id']) for v in employees.values() if v['user_id']]
    ph_emp   = ','.join(['%s'] * len(emp_ids))

    # ── Daily assignments ─────────────────────────────────────────────────────
    assign_rows = []
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT DATE(tc.created AT TIME ZONE 'Asia/Kolkata'),
                   tc.new_value, tc.author_id, t.source, COUNT(*)
            FROM support_ticketchange tc
            JOIN support_ticket t ON t.id = tc.ticket_id
            WHERE tc.field = 'Main Ticket Assignee'
              AND tc.new_value IS NOT NULL
              AND tc.new_value::bigint IN ({ph_usr})
              AND tc.created >= %s
            GROUP BY 1,2,3,4 ORDER BY 1 DESC
        """, user_ids + [since_utc])
        assign_rows = cur.fetchall()

    cur.execute(f"""
        SELECT DATE(t.created AT TIME ZONE 'Asia/Kolkata'),
               t.assigned_to_employee_id, COUNT(*)
        FROM support_ticket t
        WHERE t.source = '8'
          AND t.assigned_to_employee_id IN ({ph_emp})
          AND t.created >= %s
        GROUP BY 1,2 ORDER BY 1 DESC
    """, emp_ids + [since_utc])
    io_rows = cur.fetchall()

    # ── All closures (for pre-logoff filter) ─────────────────────────────────
    cur.execute(f"""
        SELECT tc.ticket_id,
               tc.created AT TIME ZONE 'Asia/Kolkata',
               tc.author_id
        FROM support_ticketchange tc
        WHERE tc.field = 'Status' AND tc.new_value = 'Resolved'
          AND tc.author_id IN ({ph_emp})
          AND tc.created >= %s
        ORDER BY tc.created DESC
    """, emp_ids + [since_utc])
    close_rows = cur.fetchall()

    # ── Responded & Closed ────────────────────────────────────────────────────
    responded_closed_rows = []
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT tch.ticket_id,
                   DATE(tch.created AT TIME ZONE 'Asia/Kolkata'),
                   tch.author_id,
                   CASE WHEN hr.tid IS NOT NULL THEN TRUE ELSE FALSE END
            FROM support_ticketchange tch
            LEFT JOIN LATERAL (
                SELECT cmt2.ticket_id AS tid
                FROM support_ticketcomment cmt2
                JOIN users_person p2    ON p2.user_id = cmt2.author_id
                JOIN users_employee e2  ON e2.person_id = p2.id
                WHERE cmt2.ticket_id = tch.ticket_id
                  AND cmt2.is_internal = FALSE
                  AND e2.id = tch.author_id
                LIMIT 1
            ) hr ON TRUE
            WHERE tch.field = 'Status' AND tch.new_value = 'Resolved'
              AND tch.author_id IN ({ph_emp})
              AND tch.created >= %s
        """, emp_ids + [since_utc])
        responded_closed_rows = cur.fetchall()

    # ── Responded but not highlighted ─────────────────────────────────────────
    not_highlighted_rows = []
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT t.id, t.created, t.source,
                   p.first_name, p.last_name, e.id,
                   (SELECT cmt2.comment FROM support_ticketcomment cmt2
                    WHERE cmt2.ticket_id = t.id AND cmt2.is_internal = FALSE
                      AND cmt2.author_id IN ({ph_usr})
                    ORDER BY cmt2.created DESC LIMIT 1)
            FROM support_ticket t
            JOIN support_ticketcomment cmt ON cmt.ticket_id = t.id
                 AND cmt.is_internal = FALSE
                 AND cmt.author_id::bigint IN ({ph_usr})
            JOIN users_person   p ON p.user_id = cmt.author_id
            JOIN users_employee e ON e.person_id = p.id
            WHERE t.status = '1'
              AND t.created >= NOW() - INTERVAL '60 days'
              AND NOT EXISTS (
                  SELECT 1 FROM support_ticketchange tch
                  WHERE tch.ticket_id = t.id
                    AND tch.field = 'Ops Assignee'
                    AND tch.new_value IS NOT NULL
                    AND tch.new_value != 'None'
              )
            GROUP BY t.id, t.created, t.source, p.first_name, p.last_name, e.id
            ORDER BY t.created ASC
        """, user_ids + user_ids)
        not_highlighted_rows = cur.fetchall()

    cur.close()

    # ── Process rows ─────────────────────────────────────────────────────────
    uid_to_emp = {v['user_id']: k for k, v in employees.items() if v['user_id']}

    def emp_name(eid):
        return employees.get(eid, {}).get('name', f'ID:{eid}')

    daily_assign = defaultdict(lambda: defaultdict(lambda: {'auto':0,'manual':0,'io':0}))
    for dt, assignee_uid, changed_by_emp, source, cnt in assign_rows:
        eid  = uid_to_emp.get(int(assignee_uid))
        if not eid: continue
        name = emp_name(eid)
        dts  = dt.strftime('%d %b %Y') if hasattr(dt,'strftime') else str(dt)
        key  = 'auto' if (changed_by_emp is None or changed_by_emp == eid) else 'manual'
        daily_assign[dts][name][key] += cnt

    for dt, emp_id, cnt in io_rows:
        dts = dt.strftime('%d %b %Y') if hasattr(dt,'strftime') else str(dt)
        daily_assign[dts][emp_name(emp_id)]['io'] += cnt

    pre_logoff = []
    for ticket_id, closed_ist, emp_id in close_rows:
        if closed_ist is None: continue
        name = emp_name(emp_id)
        if closed_ist.tzinfo is None: closed_ist = closed_ist.replace(tzinfo=IST)
        tdate    = closed_ist.astimezone(IST).date()
        shift_end = get_last_shift_end(roster, name, tdate)
        if shift_end is None: continue
        diff = (shift_end - closed_ist).total_seconds() / 60
        if 0 <= diff <= 60:
            pre_logoff.append({
                'ticket_id':  ticket_id,
                'closed_ist': closed_ist.astimezone(IST).strftime('%I:%M %p'),
                'name':       name,
                'dt':         tdate.strftime('%d %b %Y'),
                'date_iso':   tdate.strftime('%Y-%m-%d'),
                'shift_end':  shift_end.astimezone(IST).strftime('%I:%M %p'),
                'mins_before': round(diff),
            })

    resp_closed = defaultdict(lambda: defaultdict(lambda: {'closed':0,'with_reply':0}))
    for ticket_id, closed_dt, emp_id, has_reply in responded_closed_rows:
        dts = closed_dt.strftime('%d %b %Y') if hasattr(closed_dt,'strftime') else str(closed_dt)
        resp_closed[dts][emp_name(emp_id)]['closed'] += 1
        if has_reply: resp_closed[dts][emp_name(emp_id)]['with_reply'] += 1

    not_highlighted = []
    for ticket_id, ticket_created, source, first, last, emp_id, last_cmt in not_highlighted_rows:
        if not last_cmt or not is_agent_reply(last_cmt): continue
        name = f"{first or ''} {last or ''}".strip() or emp_name(emp_id)
        tc   = ticket_created.replace(tzinfo=timezone.utc) \
               if ticket_created.tzinfo is None else ticket_created
        hrs  = round((now_utc - tc).total_seconds()/3600, 1)
        not_highlighted.append({
            'ticket_id':   ticket_id,
            'created_str': tc.astimezone(IST).strftime('%d %b %Y %I:%M %p'),
            'source':      SRC_LABEL.get(str(source), str(source)),
            'assignee':    name,
            'hrs_open':    hrs,
            'last_reply':  last_cmt.strip()[:80],
            'date_iso':    tc.astimezone(IST).strftime('%Y-%m-%d'),
        })

    return {
        'employees':       employees,
        'daily_assign':    dict(daily_assign),
        'pre_logoff':      pre_logoff,
        'resp_closed':     dict(resp_closed),
        'not_highlighted': not_highlighted,
    }, now_utc, now_ist


# ════════════════════════════════════════════════════════════════════════════
# HTML helpers
# ════════════════════════════════════════════════════════════════════════════

PRIORITY_META = {
    'CLOSE_NOW':  ('CLOSE NOW',    '#1a6e38','#d4edda','🟢'),
    'CX_WAITING': ('CX WAITING',   '#842029','#f8d7da','🔴'),
    'NEW_OPEN':   ('NEW/NO REPLY', '#7d4e12','#fff3cd','🟠'),
    'PENDING_CX': ('PENDING REPLY','#664d03','#fff8e1','🟡'),
    'REVIEW':     ('NEEDS REVIEW', '#084298','#cfe2ff','🔵'),
    'IO':         ('IO TICKET',    '#495057','#e9ecef','⚫'),
}
CLOSE_REASON_LABEL = {
    'EMPTY':'Empty ticket','GHOST':'Ghost duplicate',
    'AC':'AC issue (Sage)','SEAT':'Seat issue (Sage)',
    'HYGIENE':'Hygiene (Sage)','TRACKING':'Tracking (Sage)',
    'SUGGESTION':'Suggestion (Sage)','MENU_ONLY':'No issue selected',
    'SILENT_24H':'Cx silent 24h+',
}
SRC_BADGE_HTML = {
    '1':'<span class="badge-src src-app">App</span>',
    '7':'<span class="badge-src src-social">Social</span>',
    '8':'<span class="badge-src src-io">IO/WA</span>',
    '9':'<span class="badge-src src-sage">Sage</span>',
}

_MON_ISO = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
            'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
def _d_iso(s):
    """'26 Mar 2026' or '26 Mar 2026 02:30 PM' → '2026-03-26'"""
    p = s.split()
    return f"{p[2]}-{_MON_ISO.get(p[1],'01')}-{p[0].zfill(2)}" if len(p) >= 3 else ''

def _pbg(p):
    return {'CLOSE_NOW':'#d4edda','CX_WAITING':'#f8d7da','NEW_OPEN':'#fff3cd',
            'PENDING_CX':'#fff8e1','REVIEW':'#cfe2ff','IO':'#e9ecef'}.get(p,'#e9ecef')

def _pfg(p):
    return {'CLOSE_NOW':'#1a6e38','CX_WAITING':'#842029','NEW_OPEN':'#7d4e12',
            'PENDING_CX':'#664d03','REVIEW':'#084298','IO':'#495057'}.get(p,'#333')


# ════════════════════════════════════════════════════════════════════════════
# HTML: Tab 1 — Ticket Queue
# ════════════════════════════════════════════════════════════════════════════

def build_tab1_html(tickets, not_highlighted, not_tagged):
    counts     = Counter(t['priority'] for t in tickets)
    total      = len(tickets)
    close_n    = counts.get('CLOSE_NOW',  0)
    cx_wait_n  = counts.get('CX_WAITING', 0)
    new_open_n = counts.get('NEW_OPEN',   0)
    pending_n  = counts.get('PENDING_CX', 0)
    review_n   = counts.get('REVIEW',     0)
    io_n       = counts.get('IO',         0)

    # Assignee breakdown
    assoc_stats = defaultdict(Counter)
    for t in tickets: assoc_stats[t['assignee']][t['priority']] += 1

    assoc_rows = ''
    for assoc in sorted(assoc_stats, key=lambda a:
                        (-assoc_stats[a].get('CLOSE_NOW',0),
                         -assoc_stats[a].get('CX_WAITING',0), a)):
        c   = assoc_stats[assoc]
        tot = sum(c.values())
        cls = 'row-cx' if c.get('CX_WAITING') else ('row-close' if c.get('CLOSE_NOW') else '')
        cn  = c.get('CLOSE_NOW',0); cxw = c.get('CX_WAITING',0)
        no  = c.get('NEW_OPEN',0);  pe  = c.get('PENDING_CX',0)
        rv  = c.get('REVIEW',0);    io  = c.get('IO',0)
        assoc_rows += (
            f'<tr class="{cls}">'
            f'<td><strong>{_esc(assoc)}</strong></td>'
            f'<td class="tc">{f"<strong>{cn}</strong>" if cn else "—"}</td>'
            f'<td class="tc">{f"<span class=\'bdg-cx\'>{cxw}</span>" if cxw else "—"}</td>'
            f'<td class="tc">{no or "—"}</td>'
            f'<td class="tc">{pe or "—"}</td>'
            f'<td class="tc">{rv or "—"}</td>'
            f'<td class="tc">{io or "—"}</td>'
            f'<td class="tc"><strong>{tot}</strong></td></tr>\n'
        )

    # Ticket table
    porder = {'CX_WAITING':0,'NEW_OPEN':1,'CLOSE_NOW':2,'PENDING_CX':3,'REVIEW':4,'IO':5}
    rows   = ''
    for t in sorted(tickets, key=lambda t:(
            porder.get(t['priority'],9),
            t['hours_open'] if t['priority'] in ('CX_WAITING','NEW_OPEN') else -t['hours_open'])):
        pm        = PRIORITY_META[t['priority']]
        badge     = (f'<span class="p-badge" style="background:{_pbg(t["priority"])};'
                     f'color:{_pfg(t["priority"])}">{pm[3]}&nbsp;{pm[0]}</span>')
        src_badge = SRC_BADGE_HTML.get(t['source'], _esc(t['src_label']))
        close_lbl = CLOSE_REASON_LABEL.get(t['close_reason'], t['close_reason'])
        hrs_str   = f"{t['hours_open']}h" if t['hours_open']<24 else f"{t['hours_open']/24:.1f}d"
        age_cls   = 'age-old' if t['hours_open']>=72 else ('age-warn' if t['hours_open']>=24 else '')
        msg_esc   = _esc(t['last_msg'])
        detail    = (f'<small class="text-muted">Agent {t["last_agent_hrs"]}h ago</small>'
                     if t['last_agent_hrs'] is not None else
                     (f'<small class="text-muted">Cx {t["last_cx_hrs"]}h ago</small>'
                      if t['last_cx_hrs'] is not None else ''))
        rows += (
            f'<tr data-priority="{t["priority"]}" data-assignee="{_esc(t["assignee"])}" data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}">\n'
            f'<td><a href="#" class="ticket-id" onclick="return false">#{t["id"]}</a></td>\n'
            f'<td>{src_badge}</td><td>{_esc(t["cat_label"])}</td>'
            f'<td>{_esc(t["assignee"])}</td>'
            f'<td class="tnw {age_cls}">{hrs_str}</td>'
            f'<td>{badge}</td><td><small>{_esc(close_lbl)}</small></td>'
            f'<td class="lmc"><span title="{msg_esc}">'
            f'{msg_esc[:60]}{"…" if len(t["last_msg"])>60 else ""}</span> {detail}</td>\n</tr>\n'
        )

    chart_labels = json.dumps(['Close Now','CX Waiting','New/No Reply','Pending','Review','IO'])
    chart_data   = json.dumps([close_n,cx_wait_n,new_open_n,pending_n,review_n,io_n])
    chart_colors = json.dumps(['#28a745','#dc3545','#fd7e14','#ffc107','#0d6efd','#adb5bd'])

    # ── Ghost tickets (source=App, no comments) ──────────────────────────────
    ghost_tickets = [t for t in tickets if t['close_reason'] == 'EMPTY']
    ghost_rows = ''
    for t in sorted(ghost_tickets, key=lambda x: -x['hours_open']):
        src_badge = SRC_BADGE_HTML.get(t['source'], _esc(t['src_label']))
        hrs_str   = f"{t['hours_open']}h" if t['hours_open']<24 else f"{t['hours_open']/24:.1f}d"
        age_cls   = 'text-danger fw-bold' if t['hours_open']>=72 else ('text-warning fw-semibold' if t['hours_open']>=24 else '')
        ghost_rows += (f'<tr data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}">'
                       f'<td><a href="#" class="ticket-id" onclick="return false">#{t["id"]}</a></td>'
                       f'<td>{src_badge}</td>'
                       f'<td>{_esc(t["assignee"])}</td>'
                       f'<td>{_esc(t["created_str"])}</td>'
                       f'<td class="tc {age_cls}">{hrs_str}</td>'
                       f'</tr>\n')
    if not ghost_rows:
        ghost_rows = '<tr><td colspan="5" class="tc text-muted py-3">No ghost tickets found ✅</td></tr>'

    # ── CX Waiting – no agent response ───────────────────────────────────────
    cx_wait_tickets = sorted([t for t in tickets if t['priority'] == 'CX_WAITING'],
                              key=lambda x: -x['hours_open'])
    cx_rows = ''
    for t in cx_wait_tickets:
        src_badge = SRC_BADGE_HTML.get(t['source'], _esc(t['src_label']))
        hrs_str   = f"{t['hours_open']}h" if t['hours_open']<24 else f"{t['hours_open']/24:.1f}d"
        age_cls   = 'text-danger fw-bold' if t['hours_open']>=72 else ('text-warning fw-semibold' if t['hours_open']>=24 else '')
        cx_hrs    = f"{t['last_cx_hrs']}h ago" if t['last_cx_hrs'] is not None else '—'
        msg_esc   = _esc(t['last_msg'])
        cx_rows += (f'<tr data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}">'
                    f'<td><a href="#" class="ticket-id" onclick="return false">#{t["id"]}</a></td>'
                    f'<td>{src_badge}</td>'
                    f'<td>{_esc(t["cat_label"])}</td>'
                    f'<td>{_esc(t["assignee"])}</td>'
                    f'<td class="tc {age_cls}">{hrs_str}</td>'
                    f'<td class="tc">{cx_hrs}</td>'
                    f'<td class="lmc"><span title="{msg_esc}">{msg_esc[:70]}{"…" if len(t["last_msg"])>70 else ""}</span></td>'
                    f'</tr>\n')
    if not cx_rows:
        cx_rows = '<tr><td colspan="7" class="tc text-muted py-3">No CX Waiting tickets ✅</td></tr>'

    # ── Not highlighted (pending) ─────────────────────────────────────────────
    nh_rows = ''
    for t in not_highlighted:
        hrs_str = f"{t['hrs_open']}h" if t['hrs_open']<24 else f"{t['hrs_open']/24:.1f}d"
        age_cls = 'text-danger fw-bold' if t['hrs_open']>=72 else ('text-warning fw-semibold' if t['hrs_open']>=24 else '')
        src_key = t['source'].lower().replace('/','').replace(' ','')
        msg     = _esc(t['last_reply'][:70]) + ('…' if len(t['last_reply'])>70 else '')
        nh_rows += (f'<tr data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}">'
                    f'<td><a href="#" class="ticket-id" onclick="return false">#{t["ticket_id"]}</a></td>'
                    f'<td>{_esc(t["created_str"])}</td>'
                    f'<td><span class="badge-src src-{src_key}">{_esc(t["source"])}</span></td>'
                    f'<td><strong>{_esc(t["assignee"])}</strong></td>'
                    f'<td class="tc {age_cls}">{hrs_str}</td>'
                    f'<td class="lmc">{msg}</td>'
                    f'</tr>\n')
    if not nh_rows:
        nh_rows = '<tr><td colspan="6" class="tc text-muted py-3">All responded tickets have been highlighted ✅</td></tr>'

    # ── Not tagged ────────────────────────────────────────────────────────────
    nt_rows = ''
    for t in not_tagged:
        src_badge = SRC_BADGE_HTML.get(t['source'], _esc(t['src_label']))
        hrs_str   = f"{t['hrs_open']}h" if t['hrs_open']<24 else f"{t['hrs_open']/24:.1f}d"
        age_cls   = 'text-danger fw-bold' if t['hrs_open']>=72 else ('text-warning fw-semibold' if t['hrs_open']>=24 else '')
        nt_rows  += (f'<tr data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}">'
                     f'<td><a href="#" class="ticket-id" onclick="return false">#{t["id"]}</a></td>'
                     f'<td>{src_badge}</td>'
                     f'<td>{_esc(t["assignee"])}</td>'
                     f'<td>{_esc(t["created_str"])}</td>'
                     f'<td class="tc {age_cls}">{hrs_str}</td>'
                     f'</tr>\n')
    if not nt_rows:
        nt_rows = '<tr><td colspan="5" class="tc text-muted py-3">All tickets have been tagged ✅</td></tr>'

    return f"""
<!-- ═══════════ TAB 1: TICKET QUEUE ═══════════ -->
<div class="summary-cards">
  <div class="card-stat card-close"><div class="num">{close_n}</div><div class="lbl">🟢 Close Now</div></div>
  <div class="card-stat card-cxwait"><div class="num">{cx_wait_n}</div><div class="lbl">🔴 CX Waiting</div></div>
  <div class="card-stat card-newopen"><div class="num">{new_open_n}</div><div class="lbl">🟠 New / No Reply</div></div>
  <div class="card-stat card-pending"><div class="num">{pending_n}</div><div class="lbl">🟡 Pending Reply</div></div>
  <div class="card-stat card-review"><div class="num">{review_n}</div><div class="lbl">🔵 Needs Review</div></div>
  <div class="card-stat card-io"><div class="num">{io_n}</div><div class="lbl">⚫ IO Tickets</div></div>
  <div class="card-stat card-total"><div class="num">{total}</div><div class="lbl">Total Open</div></div>
</div>

<div class="row mx-3 mb-3 g-3">
  <div class="col-lg-8">
    <div class="sc" style="margin:0">
      <div class="stitle">Assignee Breakdown</div>
      <div class="table-responsive">
        <table class="table table-sm table-hover mb-0" id="assocTable">
          <thead><tr>
            <th>Associate</th><th class="tc">🟢 Close Now</th>
            <th class="tc">🔴 CX Waiting</th><th class="tc">🟠 New</th>
            <th class="tc">🟡 Pending</th><th class="tc">🔵 Review</th>
            <th class="tc">⚫ IO</th><th class="tc">Total</th>
          </tr></thead>
          <tbody>{assoc_rows}</tbody>
        </table>
      </div>
    </div>
  </div>
  <div class="col-lg-4">
    <div class="sc" style="margin:0;height:100%">
      <div class="stitle">Distribution</div>
      <div style="max-width:360px;margin:0 auto"><canvas id="distChart" height="220"></canvas></div>
    </div>
  </div>
</div>

<div class="sc" style="border-left:4px solid #dc3545">
  <div class="stitle">🔴 CX Waiting — No Agent Response ({len(cx_wait_tickets)} tickets)</div>
  <div class="ssub">Customer replied last; agent has not responded yet. Sorted by longest waiting first.</div>
  <div class="table-responsive">
    <table id="cxWaitTable" class="table table-sm table-hover" style="width:100%">
      <thead><tr>
        <th>Ticket ID</th><th>Source</th><th>Category</th><th>Assignee</th>
        <th class="tc">Ticket Age</th><th class="tc">Cx Replied</th><th>Last Message</th>
      </tr></thead>
      <tbody>{cx_rows}</tbody>
    </table>
  </div>
</div>

<div class="sc" style="border-left:4px solid #1a8a45">
  <div class="stitle">👻 Ghost Tickets — Close Instantly ({len(ghost_tickets)} tickets)</div>
  <div class="ssub">App tickets with zero comments — customer never typed anything. Safe to close immediately.</div>
  <div class="table-responsive">
    <table id="ghostTable" class="table table-sm table-hover" style="width:100%">
      <thead><tr>
        <th>Ticket ID</th><th>Source</th><th>Assignee</th><th>Created (IST)</th><th class="tc">Open</th>
      </tr></thead>
      <tbody>{ghost_rows}</tbody>
    </table>
  </div>
</div>

<div class="sc" style="border-left:4px solid #fd7e14">
  <div class="stitle">🔕 Not Highlighted to Ops — Pending ({len(not_highlighted)} tickets)</div>
  <div class="ssub">Agent replied publicly but no Ops Assignee was ever set. Ordered oldest first.</div>
  <div class="table-responsive">
    <table id="nhTable" class="table table-sm table-hover" style="width:100%">
      <thead><tr>
        <th>Ticket ID</th><th>Created (IST)</th><th>Source</th>
        <th>Assignee</th><th class="tc">Hours Open</th><th>Last Agent Reply</th>
      </tr></thead>
      <tbody>{nh_rows}</tbody>
    </table>
  </div>
</div>

<div class="sc" style="border-left:4px solid #6f42c1">
  <div class="stitle">🏷️ Not Tagged by Agent ({len(not_tagged)} tickets)</div>
  <div class="ssub">Open tickets where agent commented publicly but never set a tag/reason. Ordered oldest first.</div>
  <div class="table-responsive">
    <table id="ntTable" class="table table-sm table-hover" style="width:100%">
      <thead><tr>
        <th>Ticket ID</th><th>Source</th><th>Assignee</th><th>Created (IST)</th><th class="tc">Open</th>
      </tr></thead>
      <tbody>{nt_rows}</tbody>
    </table>
  </div>
</div>

<div class="sc">
  <div class="stitle">All Tickets</div>
  <div class="tab-btns" id="filterTabs">
    <span class="tc-all"   ><button class="tab-btn active" data-filter="ALL"       >All ({total})</button></span>
    <span class="tc-close" ><button class="tab-btn" data-filter="CLOSE_NOW" >🟢 Close Now ({close_n})</button></span>
    <span class="tc-cx"    ><button class="tab-btn" data-filter="CX_WAITING">🔴 CX Waiting ({cx_wait_n})</button></span>
    <span class="tc-new"   ><button class="tab-btn" data-filter="NEW_OPEN"  >🟠 New ({new_open_n})</button></span>
    <span class="tc-pend"  ><button class="tab-btn" data-filter="PENDING_CX">🟡 Pending ({pending_n})</button></span>
    <span class="tc-rev"   ><button class="tab-btn" data-filter="REVIEW"    >🔵 Review ({review_n})</button></span>
    <span class="tc-io"    ><button class="tab-btn" data-filter="IO"        >⚫ IO ({io_n})</button></span>
  </div>
  <div class="mb-2 d-flex align-items-center gap-2">
    <label class="text-muted" style="font-size:.8rem;white-space:nowrap">Filter by agent:</label>
    <select id="assocFilter" class="form-select form-select-sm" style="max-width:220px">
      <option value="">— All agents —</option>
    </select>
  </div>
  <div class="table-responsive">
    <table id="ticketTable" class="table table-sm table-hover" style="width:100%">
      <thead><tr>
        <th>Ticket ID</th><th>Source</th><th>Category</th><th>Assignee</th>
        <th>Open</th><th>Priority</th><th>Reason</th><th>Last Message</th>
      </tr></thead>
      <tbody id="ticketBody">{rows}</tbody>
    </table>
  </div>
</div>

<script>
/* Chart data – consumed by main init script after Chart.js loads */
window._t1Chart = {{
  labels: {chart_labels},
  data:   {chart_data},
  colors: {chart_colors}
}};
</script>
"""


# ════════════════════════════════════════════════════════════════════════════
# HTML: Tab 2 — Agent Audit
# ════════════════════════════════════════════════════════════════════════════

def build_tab2_html(audit):
    daily_assign    = audit['daily_assign']
    pre_logoff      = audit['pre_logoff']
    resp_closed     = audit['resp_closed']
    not_highlighted = audit['not_highlighted']

    all_dates = sorted(
        set(list(daily_assign.keys()) + list(resp_closed.keys())), reverse=True)

    total_assigned  = sum(c['auto']+c['manual']+c['io']
                          for d in daily_assign.values() for c in d.values())
    total_closed    = sum(c['closed']     for d in resp_closed.values() for c in d.values())
    total_replied   = sum(c['with_reply'] for d in resp_closed.values() for c in d.values())
    total_pre       = len(pre_logoff)
    total_not_hl    = len(not_highlighted)

    # Section 1 rows
    s1 = ''
    for dt in all_dates:
        for agent in sorted(daily_assign.get(dt, {})):
            c   = daily_assign[dt][agent]
            tot = c['auto']+c['manual']+c['io']
            s1 += (f'<tr data-date="{_d_iso(dt)}" data-agent="{_esc(agent)}"><td>{_esc(dt)}</td><td><strong>{_esc(agent)}</strong></td>'
                   f'<td class="tc">{c["auto"] or "—"}</td>'
                   f'<td class="tc">{c["manual"] or "—"}</td>'
                   f'<td class="tc">{c["io"] or "—"}</td>'
                   f'<td class="tc"><strong>{tot}</strong></td></tr>\n')

    # Section 2 rows
    pl_agents = sorted(set(r['name'] for r in pre_logoff))
    pl_dates  = sorted(set(r['dt']   for r in pre_logoff), reverse=True)
    pl_agent_opts = ''.join(f'<option value="{_esc(a)}">{_esc(a)}</option>' for a in pl_agents)
    pl_date_opts  = ''.join(f'<option value="{_esc(d)}">{_esc(d)}</option>' for d in pl_dates)

    s2 = ''
    if pre_logoff:
        for r in sorted(pre_logoff, key=lambda x:(x['dt'],x['name'],x['mins_before'])):
            cls = 'text-danger fw-bold' if r['mins_before']<=30 else 'text-warning'
            s2 += (f'<tr data-date="{r["date_iso"]}" data-agent="{_esc(r["name"])}"><td>{_esc(r["dt"])}</td><td><strong>{_esc(r["name"])}</strong></td>'
                   f'<td>#{r["ticket_id"]}</td>'
                   f'<td class="tc">{r["closed_ist"]}</td>'
                   f'<td class="tc">{r["shift_end"]}</td>'
                   f'<td class="tc {cls}">{r["mins_before"]} min</td></tr>\n')
    else:
        s2 = '<tr><td colspan="6" class="tc text-muted py-3">No tickets closed within 1 hour before shift end.</td></tr>'

    # Section 3 rows
    s3 = ''
    for dt in all_dates:
        for agent in sorted(resp_closed.get(dt, {})):
            c    = resp_closed[dt][agent]
            pct  = round(c['with_reply']/c['closed']*100) if c['closed'] else 0
            bar  = (f'<div style="display:flex;align-items:center;gap:6px">'
                    f'<div style="flex:1;background:#e9ecef;border-radius:4px;height:8px;min-width:60px">'
                    f'<div style="width:{pct}%;background:#0d6efd;border-radius:4px;height:8px"></div>'
                    f'</div><span style="font-size:.78rem;color:#555">{pct}%</span></div>')
            s3 += (f'<tr data-date="{_d_iso(dt)}" data-agent="{_esc(agent)}"><td>{_esc(dt)}</td><td><strong>{_esc(agent)}</strong></td>'
                   f'<td class="tc"><strong>{c["closed"]}</strong></td>'
                   f'<td class="tc text-success">{c["with_reply"]}</td>'
                   f'<td class="tc text-muted">{c["closed"]-c["with_reply"]}</td>'
                   f'<td style="min-width:140px">{bar}</td></tr>\n')

    # Section 4 rows
    s4 = ''
    if not_highlighted:
        for t in not_highlighted:
            hrs_s   = f"{t['hrs_open']}h" if t['hrs_open']<24 else f"{t['hrs_open']/24:.1f}d"
            age_cls = 'text-danger fw-bold' if t['hrs_open']>=72 else (
                      'text-warning fw-semibold' if t['hrs_open']>=24 else '')
            msg     = _esc(t['last_reply'][:70]) + ('…' if len(t['last_reply'])>70 else '')
            src_key = t['source'].lower().replace('/','').replace(' ','')
            s4 += (f'<tr data-date="{t["date_iso"]}" data-agent="{_esc(t["assignee"])}"><td>#{t["ticket_id"]}</td>'
                   f'<td>{_esc(t["created_str"])}</td>'
                   f'<td><span class="badge-src src-{src_key}">{_esc(t["source"])}</span></td>'
                   f'<td><strong>{_esc(t["assignee"])}</strong></td>'
                   f'<td class="tc {age_cls}">{hrs_s}</td>'
                   f'<td class="lmc">{msg}</td></tr>\n')
    else:
        s4 = '<tr><td colspan="6" class="tc text-muted py-3">All responded tickets have been highlighted ✅</td></tr>'

    return f"""
<!-- ═══════════ TAB 2: AGENT AUDIT ═══════════ -->
<div class="summary-cards">
  <div class="card-stat" style="--card-color:#0d6efd"><div class="num">{total_assigned}</div><div class="lbl">📋 Tickets Assigned</div></div>
  <div class="card-stat" style="--card-color:#1a8a45"><div class="num">{total_closed}</div><div class="lbl">✅ Tickets Closed</div></div>
  <div class="card-stat" style="--card-color:#1a8a45"><div class="num">{total_replied}</div><div class="lbl">💬 Responded & Closed</div></div>
  <div class="card-stat" style="--card-color:#fd7e14"><div class="num">{total_pre}</div><div class="lbl">⚠️ Pre-Logoff Releases</div></div>
  <div class="card-stat" style="--card-color:#dc3545"><div class="num">{total_not_hl}</div><div class="lbl">🔕 Not Highlighted</div></div>
</div>

<div class="sc">
  <div class="stitle">📋 Daily Tickets Assigned Per Associate</div>
  <div class="ssub">Auto = system/Sage auto-assigned &nbsp;|&nbsp; Manual = assigned by another agent or TL &nbsp;|&nbsp; IO = self-raised via WhatsApp</div>
  <div class="table-responsive">
    <table id="t1" class="table table-sm table-hover" style="width:100%">
      <thead><tr><th>Date</th><th>Associate</th>
        <th class="tc">Auto-Assigned</th><th class="tc">Manually Assigned</th>
        <th class="tc">IO Self-Raised</th><th class="tc">Total</th></tr></thead>
      <tbody>{s1}</tbody>
    </table>
  </div>
</div>

<div class="sc">
  <div class="stitle">⚠️ Tickets Closed Within 1 Hour Before Logoff</div>
  <div class="ssub">Tickets closed in the 60 min before scheduled shift end. Under 30 min = red (possible ticket dumping).</div>
  <div class="note-box">Shift end times are read from <strong>roster.json</strong>. Associates not in this week's roster will not appear.</div>
  <div class="d-flex gap-2 mb-2 flex-wrap">
    <div class="d-flex align-items-center gap-1">
      <label class="text-muted" style="font-size:.8rem;white-space:nowrap">Agent:</label>
      <select id="t2agentFilter" class="form-select form-select-sm" style="min-width:170px">
        <option value="">— All agents —</option>
        {pl_agent_opts}
      </select>
    </div>
    <div class="d-flex align-items-center gap-1">
      <label class="text-muted" style="font-size:.8rem;white-space:nowrap">Date:</label>
      <select id="t2dateFilter" class="form-select form-select-sm" style="min-width:150px">
        <option value="">— All dates —</option>
        {pl_date_opts}
      </select>
    </div>
  </div>
  <div class="table-responsive">
    <table id="t2" class="table table-sm table-hover" style="width:100%">
      <thead><tr><th>Date</th><th>Associate</th><th>Ticket ID</th>
        <th class="tc">Closed At</th><th class="tc">Shift End</th><th class="tc">Mins Before End</th></tr></thead>
      <tbody>{s2}</tbody>
    </table>
  </div>
</div>

<div class="sc">
  <div class="stitle">✅ Tickets Responded and Closed Each Day</div>
  <div class="ssub">Closed = status set to Resolved. Responded = agent added a public reply before closing. % bar shows response rate.</div>
  <div class="table-responsive">
    <table id="t3" class="table table-sm table-hover" style="width:100%">
      <thead><tr><th>Date</th><th>Associate</th>
        <th class="tc">Total Closed</th><th class="tc text-success">Responded Before Close</th>
        <th class="tc text-muted">Closed Without Reply</th><th>Response Rate</th></tr></thead>
      <tbody>{s3}</tbody>
    </table>
  </div>
</div>

<div class="sc">
  <div class="stitle">🔕 Open Tickets: Agent Responded But Did Not Highlight to Ops</div>
  <div class="ssub">Agent replied publicly but no Ops Assignee was ever set. Ordered <strong>oldest first</strong>.</div>
  <div class="table-responsive">
    <table id="t4" class="table table-sm table-hover" style="width:100%">
      <thead><tr><th>Ticket ID</th><th>Created (IST)</th><th>Source</th>
        <th>Assignee</th><th class="tc">Hours Open</th><th>Last Agent Reply</th></tr></thead>
      <tbody>{s4}</tbody>
    </table>
  </div>
</div>

"""


# ════════════════════════════════════════════════════════════════════════════
# Build full HTML
# ════════════════════════════════════════════════════════════════════════════

def build_html(tickets, not_tagged, audit, now_utc, now_ist, refresh_secs=0):
    updated_str  = now_ist.strftime('%d %b %Y  %I:%M:%S %p IST')
    total        = len(tickets)
    tab1_html    = build_tab1_html(tickets, audit['not_highlighted'], not_tagged)
    tab2_html    = build_tab2_html(audit)
    auto_reload  = (f'<script>setTimeout(function(){{location.reload();}},{refresh_secs*1000});</script>'
                    if refresh_secs > 0 else '')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cityflo CS Dashboard</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
<style>
body{{font-family:'Segoe UI',sans-serif;background:#f4f6fb;font-size:.88rem;}}

/* ── Top bar ── */
.topbar{{background:linear-gradient(135deg,#0f3460,#1a6e8a);color:#fff;padding:0 24px;
         display:flex;align-items:stretch;justify-content:space-between;}}
.topbar-left{{padding:14px 0;}}
.topbar h1{{font-size:1.2rem;font-weight:700;margin:0;letter-spacing:.5px;}}
.topbar .updated{{font-size:.78rem;opacity:.8;}}

/* ── Main nav tabs ── */
.main-nav{{display:flex;align-items:stretch;gap:4px;padding:0 4px;}}
.main-nav-btn{{
  border:none;background:transparent;color:rgba(255,255,255,.7);
  font-size:.88rem;font-weight:600;padding:0 22px;cursor:pointer;
  border-bottom:3px solid transparent;transition:all .15s;
  display:flex;align-items:center;gap:6px;
}}
.main-nav-btn:hover{{color:#fff;background:rgba(255,255,255,.08);}}
.main-nav-btn.active{{color:#fff;border-bottom-color:#fff;background:rgba(255,255,255,.12);}}

/* ── Summary cards ── */
.summary-cards{{display:flex;gap:12px;padding:16px 20px;flex-wrap:wrap;}}
.card-stat{{flex:1 1 130px;border-radius:10px;padding:14px 18px;
            box-shadow:0 2px 8px rgba(0,0,0,.08);text-align:center;background:#fff;
            border-top:4px solid var(--card-color);}}
.card-stat .num{{font-size:2rem;font-weight:800;color:var(--card-color);line-height:1.1;}}
.card-stat .lbl{{font-size:.72rem;font-weight:600;text-transform:uppercase;
                 letter-spacing:.5px;color:#555;margin-top:2px;}}
.card-close  {{--card-color:#1a8a45}} .card-cxwait {{--card-color:#dc3545}}
.card-newopen{{--card-color:#fd7e14}} .card-pending{{--card-color:#c9a800}}
.card-review {{--card-color:#0d6efd}} .card-io     {{--card-color:#adb5bd}}
.card-total  {{--card-color:#495057}}

/* ── Section card ── */
.sc{{background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);
     margin:0 20px 20px;padding:18px 20px;}}
.stitle{{font-size:.95rem;font-weight:700;color:#0f3460;
         border-bottom:2px solid #e8eef5;padding-bottom:6px;margin-bottom:12px;}}
.ssub{{font-size:.78rem;color:#888;margin-bottom:12px;}}
.note-box{{background:#fff8e1;border-left:4px solid #ffc107;
           padding:8px 14px;margin-bottom:14px;font-size:.82rem;border-radius:0 6px 6px 0;}}

/* ── Priority tab filter ── */
.tab-btns{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;}}
.tab-btn{{border:2px solid #dee2e6;border-radius:20px;padding:4px 14px;
          cursor:pointer;font-size:.78rem;font-weight:600;background:#f8f9fa;
          color:#555;transition:all .15s;}}
.tab-btn:hover{{background:#e9ecef;}}
.tab-btn.active{{color:#fff!important;border-color:transparent;}}
.tc-all  .tab-btn.active{{background:#495057;}}
.tc-close.tab-btn.active,.tc-close .tab-btn.active{{background:#1a8a45;}}
.tc-cx   .tab-btn.active{{background:#dc3545;}}
.tc-new  .tab-btn.active{{background:#fd7e14;}}
.tc-pend .tab-btn.active{{background:#c9a800;}}
.tc-rev  .tab-btn.active{{background:#0d6efd;}}
.tc-io   .tab-btn.active{{background:#868e96;}}

/* ── Badges ── */
.p-badge{{display:inline-block;font-size:.7rem;font-weight:700;padding:2px 8px;
          border-radius:10px;white-space:nowrap;}}
.badge-src{{font-size:.68rem;font-weight:700;padding:1px 6px;border-radius:4px;display:inline-block;}}
.src-app   {{background:#d1ecf1;color:#0c5460;}}
.src-sage  {{background:#cce5ff;color:#004085;}}
.src-io,.src-iowa{{background:#e2e3e5;color:#383d41;}}
.src-social{{background:#fff3cd;color:#856404;}}

/* ── Tables ── */
.ticket-id{{font-weight:700;color:#0f3460;text-decoration:none;font-size:.85rem;}}
.ticket-id:hover{{text-decoration:underline;}}
.lmc{{max-width:260px;overflow:hidden;color:#555;}}
.tc {{text-align:center;}}
.tnw{{white-space:nowrap;}}
.age-warn{{color:#e67e22;font-weight:600;}} .age-old{{color:#c0392b;font-weight:700;}}
table.dataTable thead{{background:#f1f4f8;}}
table.dataTable tbody tr:hover{{background:#f0f4ff!important;}}
.row-cx   td{{background:#fff5f5!important;}}
.row-close td{{background:#f6fff8!important;}}
.bdg-cx{{background:#dc3545;color:#fff;border-radius:10px;padding:1px 8px;
         font-size:.75rem;font-weight:700;}}
/* ── Global filter bar ── */
#gFilterBar{{
  display:flex;align-items:center;gap:10px;padding:8px 20px;
  background:#fff;border-bottom:2px solid #e0e5ef;flex-wrap:wrap;
  position:sticky;top:0;z-index:999;box-shadow:0 2px 6px rgba(0,0,0,.06);
}}
#gFilterBar .flabel{{font-size:.82rem;font-weight:700;color:#0f3460;white-space:nowrap;}}
#gFilterBar .fg{{display:flex;align-items:center;gap:5px;}}
#gFilterBar .fg label{{font-size:.78rem;color:#555;white-space:nowrap;font-weight:600;}}
#gFilterBar .form-control-sm,#gFilterBar .form-select-sm{{font-size:.8rem;height:30px;padding:2px 8px;}}
#gFilterBar .active-badge{{
  background:#0d6efd;color:#fff;font-size:.72rem;font-weight:700;
  padding:2px 8px;border-radius:10px;display:none;
}}

@media(max-width:768px){{
  .summary-cards{{gap:8px;padding:10px;}}
  .card-stat .num{{font-size:1.5rem;}}
  .sc{{margin:0 8px 14px;padding:12px;}}
  #gFilterBar{{gap:6px;padding:6px 10px;}}
}}
</style>
</head>
<body>

<!-- Top bar with navigation tabs -->
<div class="topbar">
  <div class="topbar-left">
    <h1>🚌 Cityflo CS Dashboard</h1>
    <div class="updated">Updated: {updated_str} &nbsp;|&nbsp; {total} open tickets</div>
  </div>
  <div class="main-nav">
    <button class="main-nav-btn active" onclick="switchTab(1,this)" id="nav1">
      🎫 Ticket Queue
    </button>
    <button class="main-nav-btn" onclick="switchTab(2,this)" id="nav2">
      📊 Agent Audit <span style="font-size:.72rem;opacity:.8">(last {AUDIT_DAYS}d)</span>
    </button>
    <button class="btn btn-sm btn-outline-light my-auto ms-3"
            onclick="location.reload()" style="height:32px">⟳ Refresh</button>
  </div>
</div>

<!-- Global filter bar -->
<div id="gFilterBar">
  <span class="flabel">🔍 Filter:</span>
  <div class="fg">
    <label>From</label>
    <input type="date" id="gDateFrom" class="form-control form-control-sm" style="width:140px">
  </div>
  <div class="fg">
    <label>To</label>
    <input type="date" id="gDateTo" class="form-control form-control-sm" style="width:140px">
  </div>
  <div class="fg">
    <label>Agent</label>
    <select id="gAgent" class="form-select form-select-sm" style="min-width:180px">
      <option value="">— All agents —</option>
    </select>
  </div>
  <button id="gClear" class="btn btn-sm btn-outline-secondary" style="height:30px;font-size:.78rem">✕ Clear</button>
  <span id="gActiveBadge" class="active-badge">Filter active</span>
</div>

<!-- Tab 1 content -->
<div id="view1">
{tab1_html}
</div>

<!-- Tab 2 content -->
<div id="view2" style="display:none">
{tab2_html}
</div>

<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script>
// ── Global filter state ──────────────────────────────────────────────────
var gFrom='', gTo='', gAgent='', t1PF='ALL', t1AF='';

// Unified DataTable row filter — runs for every table on draw
$.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {{
  var $r = $(settings.nTable).DataTable().row(dataIndex).node();
  if (!$r) return true;
  var $row = $($r);
  // Date range
  if (gFrom || gTo) {{
    var d = $row.data('date') || '';
    if (d) {{
      if (gFrom && d < gFrom) return false;
      if (gTo   && d > gTo)   return false;
    }}
  }}
  // Agent
  if (gAgent) {{
    var a = $row.data('agent') || '';
    if (a !== gAgent) return false;
  }}
  // Tab 1 priority / assignee sub-filters (ticketTable only)
  if (settings.nTable.id === 'ticketTable') {{
    if (t1PF !== 'ALL' && ($row.data('priority')||'') !== t1PF) return false;
    if (t1AF           && ($row.data('assignee')||'') !== t1AF) return false;
  }}
  return true;
}});

function _redrawAll() {{
  ['#ticketTable','#cxWaitTable','#ghostTable','#nhTable','#ntTable','#t1','#t2','#t3','#t4']
    .forEach(function(id){{ if ($.fn.DataTable.isDataTable(id)) $(id).DataTable().draw(); }});
  var active = gFrom || gTo || gAgent;
  $('#gActiveBadge').toggle(!!active);
}}

function switchTab(n, btn) {{
  document.getElementById('view1').style.display = n===1 ? '' : 'none';
  document.getElementById('view2').style.display = n===2 ? '' : 'none';
  document.querySelectorAll('.main-nav-btn').forEach(function(b){{b.classList.remove('active');}});
  btn.classList.add('active');
  // Recalculate column widths for Tab 2 tables on first switch
  if (n===2 && !window._tab2adj) {{
    window._tab2adj = true;
    ['#t1','#t2','#t3','#t4'].forEach(function(id){{
      if ($.fn.DataTable.isDataTable(id)) $(id).DataTable().columns.adjust();
    }});
  }}
}}

$(document).ready(function() {{
  // ── Chart.js (Tab 1 donut) ───────────────────────────────────────────────
  var ctx = document.getElementById('distChart');
  if (ctx && window._t1Chart) {{
    new Chart(ctx.getContext('2d'), {{
      type: 'doughnut',
      data: {{ labels: window._t1Chart.labels,
               datasets: [{{ data: window._t1Chart.data,
                             backgroundColor: window._t1Chart.colors,
                             borderWidth: 2, borderColor: '#fff' }}] }},
      options: {{ responsive: true, plugins: {{
        legend: {{ position:'bottom', labels:{{ font:{{size:11}}, padding:8 }} }},
        tooltip: {{ callbacks: {{ label: function(c) {{
          var t = c.dataset.data.reduce(function(a,b){{return a+b;}}, 0);
          return c.label+': '+c.parsed+' ('+Math.round(c.parsed/t*100)+'%)';
        }} }} }}
      }} }}
    }});
  }}

  // ── Tab 1 DataTables ─────────────────────────────────────────────────────
  $('#ticketTable').DataTable({{pageLength:50, order:[],
    columnDefs:[{{orderable:false,targets:[1,7]}},{{width:'80px',targets:4}}],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#cxWaitTable').DataTable({{pageLength:25, order:[[4,'desc']],
    columnDefs:[{{orderable:false,targets:[1,6]}}],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#ghostTable').DataTable({{pageLength:25, order:[[4,'desc']],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#nhTable').DataTable({{pageLength:25, order:[[1,'asc']],
    columnDefs:[{{orderable:false,targets:[2,5]}}],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#ntTable').DataTable({{pageLength:25, order:[[4,'desc']],
    columnDefs:[{{orderable:false,targets:[1]}}],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});

  // Populate Tab 1 per-section agent dropdown
  var t1agents = {{}};
  $('#ticketBody tr').each(function(){{var a=$(this).data('assignee');if(a)t1agents[a]=true;}});
  Object.keys(t1agents).sort().forEach(function(a){{
    $('#assocFilter').append('<option value="'+a+'">'+a+'</option>');
  }});

  // Tab 1 priority filter buttons
  $('#filterTabs .tab-btn').on('click',function(){{
    $('#filterTabs .tab-btn').removeClass('active');$(this).addClass('active');
    t1PF=$(this).data('filter');
    if($.fn.DataTable.isDataTable('#ticketTable'))$('#ticketTable').DataTable().draw();
  }});
  // Tab 1 agent filter dropdown
  $('#assocFilter').on('change',function(){{
    t1AF=$(this).val();
    if($.fn.DataTable.isDataTable('#ticketTable'))$('#ticketTable').DataTable().draw();
  }});

  // ── Tab 2 DataTables ─────────────────────────────────────────────────────
  $('#t1').DataTable({{pageLength:25, order:[[0,'desc'],[5,'desc']],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  var dt2=$('#t2').DataTable({{pageLength:25, order:[[0,'desc'],[5,'asc']],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#t3').DataTable({{pageLength:25, order:[[0,'desc'],[2,'desc']],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});
  $('#t4').DataTable({{pageLength:50, order:[[1,'asc']],
    language:{{search:'Search:',lengthMenu:'Show _MENU_'}}}});

  // Pre-logoff section filters (agent + date dropdowns inside section)
  function escRe(s){{return s.replace(/[.*+?^${{}}()|[\\\\]\\\\\\\\]/g,'\\\\$&');}}
  function applyT2(){{
    var ag=$('#t2agentFilter').val(), dt=$('#t2dateFilter').val();
    dt2.column(1).search(ag?'^'+escRe(ag)+'$':'',true,false);
    dt2.column(0).search(dt?'^'+escRe(dt)+'$':'',true,false);
    dt2.draw();
  }}
  $('#t2agentFilter,#t2dateFilter').on('change',applyT2);

  // ── Global filter bar ────────────────────────────────────────────────────
  // Populate agent dropdown from all data-agent rows across all tables
  var allAgents = {{}};
  $('[data-agent]').each(function(){{var a=$(this).data('agent');if(a)allAgents[a]=true;}});
  Object.keys(allAgents).sort().forEach(function(a){{
    $('#gAgent').append('<option value="'+a+'">'+a+'</option>');
  }});

  $('#gDateFrom,#gDateTo').on('change',function(){{
    gFrom=$('#gDateFrom').val(); gTo=$('#gDateTo').val(); _redrawAll();
  }});
  $('#gAgent').on('change',function(){{ gAgent=$(this).val(); _redrawAll(); }});
  $('#gClear').on('click',function(){{
    gFrom=''; gTo=''; gAgent='';
    $('#gDateFrom,#gDateTo').val(''); $('#gAgent').val('');
    _redrawAll();
  }});
}});
</script>
{auto_reload}
</body>
</html>"""


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

def generate(refresh_secs=0):
    print('Loading roster…', flush=True)
    try:
        roster = load_roster()
    except Exception as e:
        print(f'  WARN: roster.json not found — pre-logoff detection disabled. ({e})')
        roster = {}

    print('Connecting to DB…', flush=True)
    try:
        conn = psycopg2.connect(DB_URL)
    except Exception as e:
        print(f'ERROR connecting: {e}'); return False

    try:
        print('  Fetching ticket queue…', flush=True)
        tickets, not_tagged, now_utc, now_ist = fetch_ticket_data(conn)
        print(f'  {len(tickets)} open tickets loaded.')

        print('  Fetching agent audit data…', flush=True)
        audit, _, _ = fetch_audit_data(conn, roster)
        print(f'  Audit data loaded.')
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f'ERROR: {e}'); conn.close(); return False
    finally:
        conn.close()

    print('  Generating dashboard…', flush=True)
    html = build_html(tickets, not_tagged, audit, now_utc, now_ist, refresh_secs)
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Saved → {OUT_FILE}', flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description='Cityflo CS Dashboard')
    parser.add_argument('--watch', nargs='?', const=5, type=int, metavar='MINUTES',
                        help='Auto-refresh interval in minutes (default 5)')
    args = parser.parse_args()

    refresh_secs = args.watch * 60 if args.watch else 0
    ok = generate(refresh_secs)
    if ok:
        webbrowser.open(f'file:///{OUT_FILE.replace(chr(92), "/")}')

    if args.watch:
        interval = args.watch * 60
        print(f'\n  Auto-refresh every {args.watch} min. Press Ctrl+C to stop.\n', flush=True)
        try:
            while True:
                time.sleep(interval)
                print(f'\n[{datetime.now(IST).strftime("%H:%M:%S")}] Refreshing…', flush=True)
                generate(refresh_secs)
        except KeyboardInterrupt:
            print('\nStopped.')


if __name__ == '__main__':
    main()
