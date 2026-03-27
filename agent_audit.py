# -*- coding: utf-8 -*-
"""
Cityflo Agent Audit Dashboard
================================
Four sections per associate, last 7 days + today:

  1. DAILY ASSIGNMENTS  — Tickets assigned each day (auto + manual + IO self-raised)
  2. PRE-LOGOFF RELEASES — Tickets closed within 1 hour before scheduled shift end
  3. RESPONDED & CLOSED  — Tickets the agent both replied to AND closed each day
  4. RESPONDED, NOT HIGHLIGHTED — Open tickets: agent replied but never set an Ops Assignee
                                   (ordered oldest → newest)

Usage:
    python agent_audit.py              # generate and open browser
    python agent_audit.py --watch      # auto-refresh every 5 minutes
    python agent_audit.py --watch 3    # custom interval (minutes)
"""
import sys, io, json, os, re, webbrowser, argparse, time
import psycopg2
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta, date, time as dtime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_URL   = ('postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster'
            '.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/'
            'cityflo_final_backend?sslmode=prefer')
IST      = timezone(timedelta(hours=5, minutes=30))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(BASE_DIR, 'agent_audit.html')
ROSTER_FILE = os.path.join(BASE_DIR, 'roster.json')

DAYS = 7   # how many days back to show


# ════════════════════════════════════════════════════════════════════════════
# Roster helpers
# ════════════════════════════════════════════════════════════════════════════

def load_roster():
    with open(ROSTER_FILE, encoding='utf-8') as f:
        return json.load(f)


def _parse_t(s):
    """Parse 'HH:MM' string into a time object."""
    h, m = map(int, s.split(':'))
    return dtime(h, m)


def _norm_name(s):
    return re.sub(r'\s+', ' ', (s or '').lower().strip())


def get_shift_sessions(roster, name_db, target_date):
    """
    Return list of (start_datetime_ist, end_datetime_ist, label) for the associate
    on target_date (date object). Returns [] if associate not found.
    """
    # Check weekend rosters first
    target_str = target_date.strftime('%Y-%m-%d')
    entry = None
    for wr in roster.get('weekend_rosters', []):
        if wr.get('date') == target_str:
            for a in wr.get('associates', []):
                if _norm_name(name_db) in _norm_name(a.get('name_db', '')):
                    entry = a; break
            if entry: break

    # Fall back to regular weekday roster
    if not entry:
        for a in roster.get('associates', []):
            if _norm_name(name_db) in _norm_name(a.get('name_db', '')):
                entry = a; break

    if not entry:
        return []

    sessions = []
    for slot_key, label in [('morning', 'Morning'), ('evening', 'Evening')]:
        slot = entry.get(slot_key)
        if not slot:
            continue
        start_t = _parse_t(slot['start'])
        end_t   = _parse_t(slot['end'])
        start_dt = datetime.combine(target_date, start_t, tzinfo=IST)
        end_dt   = datetime.combine(target_date, end_t,   tzinfo=IST)
        sessions.append((start_dt, end_dt, label))
    return sessions


def get_last_shift_end(roster, name_db, target_date):
    """Return the last scheduled shift-end datetime for associate on target_date."""
    sessions = get_shift_sessions(roster, name_db, target_date)
    if not sessions:
        return None
    return max(s[1] for s in sessions)


# ════════════════════════════════════════════════════════════════════════════
# Agent boilerplate helpers (same as ticket_dashboard)
# ════════════════════════════════════════════════════════════════════════════

AGENT_PHRASES = [
    'good morning','good afternoon','good evening','we apologize',
    'inconvenience','highlighted the issue','relevant team','allow us',
    'sorry to hear','sincerely apologize','i have escalated','we will check',
    'we will look into','please allow','kindly allow','we will get back',
    'thank you for','thanks for reaching','we have noted','rest assured',
    'we will resolve','we will update you','we will inform',
    'i am sorry to hear','we kindly request','we have shared','we have escalated',
    'i have highlighted','i have addressed',
]

def is_agent_reply(comment):
    if not comment: return False
    try:
        json.loads(comment); return False
    except Exception:
        return any(p in comment.lower() for p in AGENT_PHRASES)


# ════════════════════════════════════════════════════════════════════════════
# DB fetch
# ════════════════════════════════════════════════════════════════════════════

def fetch_data(roster):
    now_utc  = datetime.now(timezone.utc)
    now_ist  = now_utc.astimezone(IST)
    since_utc = now_utc - timedelta(days=DAYS + 1)

    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # ── 1. Get all CS associates who have been active in last 7+ days ────────
    cur.execute("""
        SELECT DISTINCT e.id as emp_id,
               p.first_name,
               p.last_name,
               p.user_id,
               p.id as person_id
        FROM support_ticketchange tc
        JOIN users_employee e  ON e.id = tc.author_id
        JOIN users_person   p  ON p.id = e.person_id
        WHERE tc.created >= %s
          AND tc.field = 'Status'
    """, (since_utc,))
    employees = {}   # emp_id -> {name, user_id, person_id}
    for emp_id, first, last, user_id, person_id in cur.fetchall():
        name = f"{first or ''} {last or ''}".strip()
        employees[emp_id] = {'name': name, 'user_id': user_id, 'person_id': person_id}

    if not employees:
        cur.close(); conn.close()
        return {}, now_utc, now_ist

    emp_ids     = list(employees.keys())
    user_ids    = [str(v['user_id']) for v in employees.values() if v['user_id']]
    ph_emp      = ','.join(['%s'] * len(emp_ids))

    # ── 2. SECTION 1 — Daily assignments via 'Main Ticket Assignee' ──────────
    # new_value = users_person.user_id
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT
                DATE(tc.created AT TIME ZONE 'Asia/Kolkata') as dt,
                tc.new_value                                   as assignee_uid,
                tc.author_id                                   as changed_by_emp,
                t.source,
                COUNT(*)                                       as cnt
            FROM support_ticketchange tc
            JOIN support_ticket t ON t.id = tc.ticket_id
            WHERE tc.field = 'Main Ticket Assignee'
              AND tc.new_value IS NOT NULL
              AND tc.new_value::bigint IN ({ph_usr})
              AND tc.created >= %s
            GROUP BY dt, tc.new_value, tc.author_id, t.source
            ORDER BY dt DESC
        """, user_ids + [since_utc])
        assign_rows = cur.fetchall()
    else:
        assign_rows = []

    # IO tickets (source=8): assigned via assigned_to_employee_id
    cur.execute(f"""
        SELECT
            DATE(t.created AT TIME ZONE 'Asia/Kolkata') as dt,
            t.assigned_to_employee_id                   as emp_id,
            COUNT(*)                                     as cnt
        FROM support_ticket t
        WHERE t.source = '8'
          AND t.assigned_to_employee_id IN ({ph_emp})
          AND t.created >= %s
        GROUP BY dt, t.assigned_to_employee_id
        ORDER BY dt DESC
    """, emp_ids + [since_utc])
    io_rows = cur.fetchall()

    # ── 3. SECTION 2 — All ticket closures (for pre-logoff detection) ────────
    cur.execute(f"""
        SELECT
            tc.ticket_id,
            tc.created AT TIME ZONE 'Asia/Kolkata' as closed_ist,
            tc.author_id                            as emp_id
        FROM support_ticketchange tc
        WHERE tc.field = 'Status'
          AND tc.new_value = 'Resolved'
          AND tc.author_id IN ({ph_emp})
          AND tc.created >= %s
        ORDER BY tc.created DESC
    """, emp_ids + [since_utc])
    close_rows = cur.fetchall()

    # ── 4. SECTION 3 — Responded AND closed same day ─────────────────────────
    # Get all tickets closed by associate in last 7 days
    # Then check if that associate also has a public comment on the ticket
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT
                tch.ticket_id,
                DATE(tch.created AT TIME ZONE 'Asia/Kolkata') as closed_dt,
                tch.author_id                                  as emp_id,
                CASE WHEN has_reply.tid IS NOT NULL THEN TRUE ELSE FALSE END as has_reply
            FROM support_ticketchange tch
            LEFT JOIN LATERAL (
                SELECT cmt2.ticket_id as tid
                FROM support_ticketcomment cmt2
                JOIN users_person p2 ON p2.user_id = cmt2.author_id
                JOIN users_employee e2 ON e2.person_id = p2.id
                WHERE cmt2.ticket_id = tch.ticket_id
                  AND cmt2.is_internal = FALSE
                  AND e2.id = tch.author_id
                LIMIT 1
            ) has_reply ON TRUE
            WHERE tch.field = 'Status'
              AND tch.new_value = 'Resolved'
              AND tch.author_id IN ({ph_emp})
              AND tch.created >= %s
        """, emp_ids + [since_utc])
        responded_closed_rows = cur.fetchall()
    else:
        responded_closed_rows = []

    # ── 5. SECTION 4 — Responded but NOT highlighted (open tickets) ──────────
    # Open tickets where:
    #   a) agent has a public comment
    #   b) no 'Ops Assignee' was ever set (new_value IS NOT NULL)
    # Ordered by ticket created ASC (oldest first)
    if user_ids:
        ph_usr = ','.join(['%s'] * len(user_ids))
        cur.execute(f"""
            SELECT
                t.id          as ticket_id,
                t.created     as ticket_created,
                t.source,
                p.first_name,
                p.last_name,
                e.id          as emp_id,
                (
                    SELECT cmt2.comment
                    FROM support_ticketcomment cmt2
                    WHERE cmt2.ticket_id = t.id
                      AND cmt2.is_internal = FALSE
                      AND cmt2.author_id IN ({ph_usr})
                    ORDER BY cmt2.created DESC LIMIT 1
                ) as last_agent_comment
            FROM support_ticket t
            JOIN support_ticketcomment cmt ON cmt.ticket_id = t.id
                AND cmt.is_internal = FALSE
                AND cmt.author_id::bigint IN ({ph_usr})
            JOIN users_person p ON p.user_id = cmt.author_id
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
    else:
        not_highlighted_rows = []

    cur.close(); conn.close()

    # ════════════════════════════════════════════════════════════════════════
    # Build lookup: user_id -> emp_id and reverse
    # ════════════════════════════════════════════════════════════════════════
    uid_to_emp = {}   # user_id (int) -> emp_id
    for emp_id, info in employees.items():
        if info['user_id']:
            uid_to_emp[info['user_id']] = emp_id

    def emp_name(emp_id):
        return employees.get(emp_id, {}).get('name', f'ID:{emp_id}')

    def uid_emp_name(uid):
        if uid is None: return 'Unknown'
        eid = uid_to_emp.get(int(uid))
        return emp_name(eid) if eid else f'UID:{uid}'

    # ════════════════════════════════════════════════════════════════════════
    # Section 1: Daily assignment table
    # Structure: {date_str: {agent_name: {auto:N, manual:N, io:N}}}
    # ════════════════════════════════════════════════════════════════════════
    daily_assign = defaultdict(lambda: defaultdict(lambda: {'auto': 0, 'manual': 0, 'io': 0}))

    for dt, assignee_uid, changed_by_emp, source, cnt in assign_rows:
        agent_uid = int(assignee_uid)
        agent_emp = uid_to_emp.get(agent_uid)
        if not agent_emp: continue
        name  = emp_name(agent_emp)
        dt_str = dt.strftime('%d %b %Y') if hasattr(dt, 'strftime') else str(dt)

        # Self-assigned = changed_by_emp is None (system/auto) or == same person
        if changed_by_emp is None or (changed_by_emp == agent_emp):
            daily_assign[dt_str][name]['auto'] += cnt
        else:
            daily_assign[dt_str][name]['manual'] += cnt

    for dt, emp_id, cnt in io_rows:
        name = emp_name(emp_id)
        dt_str = dt.strftime('%d %b %Y') if hasattr(dt, 'strftime') else str(dt)
        daily_assign[dt_str][name]['io'] += cnt

    # ════════════════════════════════════════════════════════════════════════
    # Section 2: Pre-logoff releases
    # ════════════════════════════════════════════════════════════════════════
    pre_logoff = []   # list of {ticket_id, closed_ist, emp_id, name, dt, shift_end, minutes_before}

    for ticket_id, closed_ist, emp_id in close_rows:
        name = emp_name(emp_id)
        if closed_ist is None: continue
        if closed_ist.tzinfo is None:
            closed_ist = closed_ist.replace(tzinfo=IST)
        target_dt = closed_ist.astimezone(IST).date()

        shift_end = get_last_shift_end(roster, name, target_dt)
        if shift_end is None: continue

        diff_mins = (shift_end - closed_ist).total_seconds() / 60
        if 0 <= diff_mins <= 60:
            pre_logoff.append({
                'ticket_id':     ticket_id,
                'closed_ist':    closed_ist.astimezone(IST).strftime('%I:%M %p'),
                'emp_id':        emp_id,
                'name':          name,
                'dt':            target_dt.strftime('%d %b %Y'),
                'shift_end':     shift_end.astimezone(IST).strftime('%I:%M %p'),
                'mins_before':   round(diff_mins),
            })

    # ════════════════════════════════════════════════════════════════════════
    # Section 3: Responded and closed per agent per day
    # ════════════════════════════════════════════════════════════════════════
    resp_closed = defaultdict(lambda: defaultdict(lambda: {'closed': 0, 'with_reply': 0}))

    for ticket_id, closed_dt, emp_id, has_reply in responded_closed_rows:
        name   = emp_name(emp_id)
        dt_str = closed_dt.strftime('%d %b %Y') if hasattr(closed_dt, 'strftime') else str(closed_dt)
        resp_closed[dt_str][name]['closed'] += 1
        if has_reply:
            resp_closed[dt_str][name]['with_reply'] += 1

    # ════════════════════════════════════════════════════════════════════════
    # Section 4: Responded but not highlighted
    # ════════════════════════════════════════════════════════════════════════
    not_highlighted = []

    SRC = {'1': 'App', '7': 'Social', '8': 'IO/WA', '9': 'Sage'}
    for ticket_id, ticket_created, source, first, last, emp_id, last_cmt in not_highlighted_rows:
        if not last_cmt: continue
        if not is_agent_reply(last_cmt): continue  # only show if agent actually replied
        name = f"{first or ''} {last or ''}".strip() or emp_name(emp_id)
        created_ist = ticket_created.replace(tzinfo=timezone.utc).astimezone(IST) \
                      if ticket_created.tzinfo is None else ticket_created.astimezone(IST)
        hrs_open = round((now_utc - ticket_created.replace(tzinfo=timezone.utc)
                          if ticket_created.tzinfo is None
                          else now_utc - ticket_created).total_seconds() / 3600, 1)
        not_highlighted.append({
            'ticket_id':     ticket_id,
            'created_str':   created_ist.strftime('%d %b %Y %I:%M %p'),
            'source':        SRC.get(str(source), str(source)),
            'assignee':      name,
            'hrs_open':      hrs_open,
            'last_reply':    last_cmt.strip()[:80],
        })

    return {
        'employees':      employees,
        'daily_assign':   dict(daily_assign),
        'pre_logoff':     pre_logoff,
        'resp_closed':    dict(resp_closed),
        'not_highlighted': not_highlighted,
    }, now_utc, now_ist


# ════════════════════════════════════════════════════════════════════════════
# HTML generation
# ════════════════════════════════════════════════════════════════════════════

def _esc(s):
    return (str(s)
            .replace('&','&amp;').replace('<','&lt;')
            .replace('>','&gt;').replace('"','&quot;'))


def build_html(data, now_utc, now_ist):
    employees      = data['employees']
    daily_assign   = data['daily_assign']
    pre_logoff     = data['pre_logoff']
    resp_closed    = data['resp_closed']
    not_highlighted = data['not_highlighted']

    updated_str = now_ist.strftime('%d %b %Y  %I:%M:%S %p IST')

    # Sorted dates (most recent first)
    all_dates = sorted(set(list(daily_assign.keys()) + list(resp_closed.keys())), reverse=True)
    all_agents = sorted(set(
        name for d in daily_assign.values() for name in d
    ) | set(
        name for d in resp_closed.values() for name in d
    ))

    # ── Section 1: Daily assignments ─────────────────────────────────────────
    s1_rows = ''
    for dt in all_dates:
        agents_on_date = {}
        for agent, counts in daily_assign.get(dt, {}).items():
            agents_on_date[agent] = counts
        for agent in sorted(agents_on_date):
            c    = agents_on_date[agent]
            tot  = c['auto'] + c['manual'] + c['io']
            s1_rows += (
                f'<tr>'
                f'<td>{_esc(dt)}</td>'
                f'<td><strong>{_esc(agent)}</strong></td>'
                f'<td class="text-center">{c["auto"]  or "—"}</td>'
                f'<td class="text-center">{c["manual"] or "—"}</td>'
                f'<td class="text-center">{c["io"]     or "—"}</td>'
                f'<td class="text-center"><strong>{tot}</strong></td>'
                f'</tr>\n'
            )

    # ── Section 2: Pre-logoff releases ───────────────────────────────────────
    s2_rows = ''
    if pre_logoff:
        pre_logoff_sorted = sorted(pre_logoff,
                                   key=lambda x: (x['dt'], x['name'], x['mins_before']))
        # Group by date + agent for summary
        pre_counts = Counter((r['dt'], r['name']) for r in pre_logoff_sorted)
        summary_printed = set()
        for r in pre_logoff_sorted:
            key = (r['dt'], r['name'])
            warn_cls = 'text-danger fw-bold' if r['mins_before'] <= 30 else 'text-warning'
            s2_rows += (
                f'<tr>'
                f'<td>{_esc(r["dt"])}</td>'
                f'<td><strong>{_esc(r["name"])}</strong></td>'
                f'<td>#{r["ticket_id"]}</td>'
                f'<td class="text-center">{r["closed_ist"]}</td>'
                f'<td class="text-center">{r["shift_end"]}</td>'
                f'<td class="text-center {warn_cls}">{r["mins_before"]} min</td>'
                f'</tr>\n'
            )
    else:
        s2_rows = '<tr><td colspan="6" class="text-center text-muted py-3">No tickets closed within 1 hour before shift end.</td></tr>'

    # ── Section 3: Responded and closed ──────────────────────────────────────
    s3_rows = ''
    for dt in all_dates:
        day_data = resp_closed.get(dt, {})
        for agent in sorted(day_data):
            c         = day_data[agent]
            closed    = c['closed']
            with_rep  = c['with_reply']
            without   = closed - with_rep
            pct       = round(with_rep / closed * 100) if closed else 0
            bar_w     = pct
            bar_html  = (
                f'<div style="display:flex;align-items:center;gap:6px">'
                f'<div style="flex:1;background:#e9ecef;border-radius:4px;height:8px;min-width:60px">'
                f'<div style="width:{bar_w}%;background:#0d6efd;border-radius:4px;height:8px"></div>'
                f'</div>'
                f'<span style="font-size:.78rem;color:#555">{pct}%</span>'
                f'</div>'
            )
            s3_rows += (
                f'<tr>'
                f'<td>{_esc(dt)}</td>'
                f'<td><strong>{_esc(agent)}</strong></td>'
                f'<td class="text-center"><strong>{closed}</strong></td>'
                f'<td class="text-center text-success">{with_rep}</td>'
                f'<td class="text-center text-muted">{without}</td>'
                f'<td style="min-width:140px">{bar_html}</td>'
                f'</tr>\n'
            )

    # ── Section 4: Not highlighted ───────────────────────────────────────────
    s4_rows = ''
    if not_highlighted:
        for t in not_highlighted:
            hrs   = t['hrs_open']
            hrs_s = f"{hrs}h" if hrs < 24 else f"{hrs/24:.1f}d"
            age_cls = 'text-danger fw-bold' if hrs >= 72 else ('text-warning fw-semibold' if hrs >= 24 else '')
            last_msg = _esc(t['last_reply'][:70]) + ('…' if len(t['last_reply']) > 70 else '')
            s4_rows += (
                f'<tr>'
                f'<td>#{t["ticket_id"]}</td>'
                f'<td>{_esc(t["created_str"])}</td>'
                f'<td><span class="badge-src src-{t["source"].lower().replace("/","").replace(" ","")}">'
                f'{_esc(t["source"])}</span></td>'
                f'<td><strong>{_esc(t["assignee"])}</strong></td>'
                f'<td class="text-center {age_cls}">{hrs_s}</td>'
                f'<td class="last-msg-cell">{last_msg}</td>'
                f'</tr>\n'
            )
    else:
        s4_rows = '<tr><td colspan="6" class="text-center text-muted py-3">All responded tickets have been highlighted. ✅</td></tr>'

    # Summary counts
    total_assigned   = sum(
        c['auto'] + c['manual'] + c['io']
        for d in daily_assign.values()
        for c in d.values()
    )
    total_pre_logoff = len(pre_logoff)
    total_closed     = sum(c['closed']    for d in resp_closed.values() for c in d.values())
    total_replied    = sum(c['with_reply'] for d in resp_closed.values() for c in d.values())
    total_not_hl     = len(not_highlighted)

    # ══════════════════════════════════════════════════════════════════════════
    # HTML template
    # ══════════════════════════════════════════════════════════════════════════
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cityflo — Agent Audit Dashboard</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
<style>
  body {{ font-family:'Segoe UI',sans-serif; background:#f4f6fb; font-size:.88rem; }}
  .topbar {{
    background: linear-gradient(135deg,#0f3460 0%,#1a6e8a 100%);
    color:#fff; padding:14px 24px;
    display:flex; align-items:center; justify-content:space-between;
  }}
  .topbar h1 {{ font-size:1.2rem; font-weight:700; margin:0; }}
  .topbar .updated {{ font-size:.78rem; opacity:.8; }}
  .summary-cards {{ display:flex; gap:12px; padding:16px 20px; flex-wrap:wrap; }}
  .card-stat {{
    flex:1 1 130px; border-radius:10px; padding:14px 18px;
    box-shadow:0 2px 8px rgba(0,0,0,.08); text-align:center; background:#fff;
    border-top:4px solid var(--cc);
  }}
  .card-stat .num {{ font-size:2rem; font-weight:800; color:var(--cc); line-height:1.1; }}
  .card-stat .lbl {{ font-size:.72rem; font-weight:600; text-transform:uppercase;
                     letter-spacing:.5px; color:#555; margin-top:2px; }}
  .c1{{--cc:#0d6efd}} .c2{{--cc:#fd7e14}} .c3{{--cc:#1a8a45}} .c4{{--cc:#dc3545}}

  .section-card {{
    background:#fff; border-radius:10px;
    box-shadow:0 2px 8px rgba(0,0,0,.07);
    margin:0 20px 24px; padding:18px 20px;
  }}
  .section-title {{
    font-size:1rem; font-weight:700; margin-bottom:4px;
    color:#0f3460;
  }}
  .section-sub {{
    font-size:.78rem; color:#888; margin-bottom:14px;
    border-bottom:2px solid #e8eef5; padding-bottom:8px;
  }}
  .badge-src {{ font-size:.68rem; font-weight:700; padding:1px 6px;
                border-radius:4px; display:inline-block; }}
  .src-app    {{ background:#d1ecf1; color:#0c5460; }}
  .src-sage   {{ background:#cce5ff; color:#004085; }}
  .src-iowa,.src-iowa {{ background:#e2e3e5; color:#383d41; }}
  .src-social {{ background:#fff3cd; color:#856404; }}
  table.dataTable thead {{ background:#f1f4f8; }}
  table.dataTable tbody tr:hover {{ background:#f0f4ff !important; }}
  .last-msg-cell {{ max-width:260px; overflow:hidden; color:#555; font-size:.82rem; }}
  .section-icon {{ font-size:1.3rem; margin-right:6px; vertical-align:middle; }}
  .note-box {{
    background:#fff8e1; border-left:4px solid #ffc107;
    padding:8px 14px; margin-bottom:14px; font-size:.82rem;
    border-radius:0 6px 6px 0;
  }}
  @media(max-width:768px){{
    .summary-cards{{gap:8px;padding:10px;}}
    .section-card{{margin:0 8px 14px;padding:12px;}}
  }}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <div>
    <h1>🚌 Cityflo — Agent Audit Dashboard</h1>
    <div class="updated">Updated: {updated_str} &nbsp;|&nbsp; Last {DAYS} days</div>
  </div>
  <button class="btn btn-sm btn-light" onclick="location.reload()">⟳ Refresh</button>
</div>

<!-- Summary cards -->
<div class="summary-cards">
  <div class="card-stat c1">
    <div class="num">{total_assigned}</div>
    <div class="lbl">📋 Tickets Assigned</div>
  </div>
  <div class="card-stat c3">
    <div class="num">{total_closed}</div>
    <div class="lbl">✅ Tickets Closed</div>
  </div>
  <div class="card-stat c3" style="--cc:#1a8a45">
    <div class="num">{total_replied}</div>
    <div class="lbl">💬 Responded & Closed</div>
  </div>
  <div class="card-stat c2">
    <div class="num">{total_pre_logoff}</div>
    <div class="lbl">⚠️ Pre-Logoff Releases</div>
  </div>
  <div class="card-stat c4">
    <div class="num">{total_not_hl}</div>
    <div class="lbl">🔕 Not Highlighted</div>
  </div>
</div>

<!-- ══ SECTION 1: Daily Assignments ══════════════════════════════════════ -->
<div class="section-card">
  <div class="section-title"><span class="section-icon">📋</span>Daily Tickets Assigned Per Associate</div>
  <div class="section-sub">
    Auto = system/Sage auto-assigned &nbsp;|&nbsp;
    Manual = assigned by another agent or TL &nbsp;|&nbsp;
    IO = tickets raised by associate directly via WhatsApp/IO
  </div>
  <div class="table-responsive">
    <table id="t1" class="table table-sm table-hover" style="width:100%">
      <thead>
        <tr>
          <th>Date</th>
          <th>Associate</th>
          <th class="text-center">Auto-Assigned</th>
          <th class="text-center">Manually Assigned</th>
          <th class="text-center">IO Self-Raised</th>
          <th class="text-center">Total</th>
        </tr>
      </thead>
      <tbody>{s1_rows}</tbody>
    </table>
  </div>
</div>

<!-- ══ SECTION 2: Pre-Logoff Releases ═══════════════════════════════════ -->
<div class="section-card">
  <div class="section-title"><span class="section-icon">⚠️</span>Tickets Released Within 1 Hour Before Scheduled Logoff</div>
  <div class="section-sub">
    Tickets closed (Resolved) in the 60 minutes before the associate's scheduled shift end.
    Under 30 min shown in red — may indicate ticket dumping before logoff.
  </div>
  <div class="note-box">
    Shift end times are read from <strong>roster.json</strong>.
    Associates not in this week's roster will not appear here.
  </div>
  <div class="table-responsive">
    <table id="t2" class="table table-sm table-hover" style="width:100%">
      <thead>
        <tr>
          <th>Date</th>
          <th>Associate</th>
          <th>Ticket ID</th>
          <th class="text-center">Closed At</th>
          <th class="text-center">Shift End</th>
          <th class="text-center">Mins Before End</th>
        </tr>
      </thead>
      <tbody>{s2_rows}</tbody>
    </table>
  </div>
</div>

<!-- ══ SECTION 3: Responded & Closed ════════════════════════════════════ -->
<div class="section-card">
  <div class="section-title"><span class="section-icon">✅</span>Tickets Responded and Closed Each Day</div>
  <div class="section-sub">
    Closed = ticket status set to Resolved by the associate.
    Responded = associate also added a public reply to the ticket before closing.
    The % bar shows what proportion of closed tickets had a response.
  </div>
  <div class="table-responsive">
    <table id="t3" class="table table-sm table-hover" style="width:100%">
      <thead>
        <tr>
          <th>Date</th>
          <th>Associate</th>
          <th class="text-center">Total Closed</th>
          <th class="text-center text-success">Responded Before Close</th>
          <th class="text-center text-muted">Closed Without Reply</th>
          <th>Response Rate</th>
        </tr>
      </thead>
      <tbody>{s3_rows}</tbody>
    </table>
  </div>
</div>

<!-- ══ SECTION 4: Responded But Not Highlighted ═════════════════════════ -->
<div class="section-card">
  <div class="section-title"><span class="section-icon">🔕</span>Open Tickets: Agent Responded But Did Not Highlight to Ops</div>
  <div class="section-sub">
    These are currently open tickets where the agent replied to the customer
    but never assigned an Ops Assignee (i.e. never highlighted the issue to the operations team).
    Ordered <strong>oldest first</strong> — these need attention first.
  </div>
  <div class="table-responsive">
    <table id="t4" class="table table-sm table-hover" style="width:100%">
      <thead>
        <tr>
          <th>Ticket ID</th>
          <th>Created (IST)</th>
          <th>Source</th>
          <th>Assignee</th>
          <th class="text-center">Hours Open</th>
          <th>Last Agent Reply</th>
        </tr>
      </thead>
      <tbody>{s4_rows}</tbody>
    </table>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script>
$(document).ready(function(){{
  $('#t1').DataTable({{pageLength:25, order:[[0,'desc'],[5,'desc']],
    language:{{search:'Filter:',lengthMenu:'Show _MENU_'}} }});
  $('#t2').DataTable({{pageLength:25, order:[[0,'desc'],[5,'asc']],
    language:{{search:'Filter:',lengthMenu:'Show _MENU_'}} }});
  $('#t3').DataTable({{pageLength:25, order:[[0,'desc'],[2,'desc']],
    language:{{search:'Filter:',lengthMenu:'Show _MENU_'}} }});
  $('#t4').DataTable({{pageLength:50, order:[[1,'asc']],
    language:{{search:'Filter:',lengthMenu:'Show _MENU_'}} }});
}});
</script>
</body>
</html>"""
    return html


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

def generate():
    print('Loading roster…', flush=True)
    try:
        roster = load_roster()
    except Exception as e:
        print(f'  WARN: Could not load roster.json — pre-logoff detection disabled. ({e})')
        roster = {}

    print('Fetching audit data…', flush=True)
    try:
        data, now_utc, now_ist = fetch_data(roster)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f'ERROR: {e}')
        return False

    print(f'  Generating dashboard…', flush=True)
    html = build_html(data, now_utc, now_ist)
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Saved → {OUT_FILE}', flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description='Cityflo Agent Audit Dashboard')
    parser.add_argument('--watch', nargs='?', const=5, type=int, metavar='MINUTES',
                        help='Auto-refresh interval in minutes (default 5)')
    args = parser.parse_args()

    ok = generate()
    if ok:
        webbrowser.open(f'file:///{OUT_FILE.replace(chr(92), "/")}')

    if args.watch:
        interval = args.watch * 60
        print(f'\n  Auto-refresh every {args.watch} min. Press Ctrl+C to stop.\n', flush=True)
        try:
            while True:
                time.sleep(interval)
                print(f'\n[{datetime.now(IST).strftime("%H:%M:%S")}] Refreshing…', flush=True)
                generate()
                webbrowser.open(f'file:///{OUT_FILE.replace(chr(92), "/")}')
        except KeyboardInterrupt:
            print('\nStopped.')


if __name__ == '__main__':
    main()
