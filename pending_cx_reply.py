# -*- coding: utf-8 -*-
# Find open tickets where agent has replied but customer hasn't responded in 24+ hours
# These are safe to auto-close — customer went silent after agent response
import sys, io, psycopg2, json
from collections import defaultdict
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

now = datetime.now(timezone.utc)

AGENT_PHRASES = [
    'good morning', 'good afternoon', 'good evening',
    'we apologize', 'sincerely apologize', 'i apologize',
    'we have noted', 'we have highlighted', 'highlighted the issue',
    'allow us some time', 'we will get back', 'we will check',
    'we will look into', 'we have shared', 'we have escalated',
    'i have escalated', 'i have highlighted', 'i have addressed',
    'rest assured', 'we take this seriously', 'thank you for',
    'thanks for reaching', 'kindly allow', 'please allow',
    'we apologize for the inconvenience', 'sorry for the inconvenience',
    'we will resolve', 'we will update you', 'we will inform',
    'i am sorry to hear', 'sorry to hear', 'i am writing to follow',
    'we kindly request you to please elaborate',
]

def is_agent_reply(comment):
    if not comment:
        return False
    t = comment.lower().strip()
    # Plain text (not JSON) that starts with agent boilerplate
    try:
        json.loads(comment)
        return False  # JSON = Sage/customer message
    except Exception:
        return any(p in t for p in AGENT_PHRASES)

def is_customer_message(comment):
    if not comment:
        return False
    # JSON with yellow bubble = customer via Sage
    try:
        data = json.loads(comment)
        msgs = []
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    msgs.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, in_yellow)
        walk(data)
        return len(msgs) > 0 and len(' '.join(msgs).strip()) > 5
    except Exception:
        # Plain text — not an agent reply = could be customer (IO/WhatsApp)
        return not is_agent_reply(comment) and len(comment.strip()) > 5

# Fetch all open tickets with all comments ordered by time
cur.execute("""
    SELECT t.id, t.source, t.info_tag, t.created,
           tc.comment, tc.is_internal, tc.created as tc_created
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1'
      AND t.created >= NOW() - INTERVAL '30 days'
    ORDER BY t.id, tc.created ASC
""")
rows = cur.fetchall()

# Group by ticket
ticket_comments = defaultdict(list)
ticket_meta = {}
for r in rows:
    tid, source, info_tag, created, comment, is_internal, tc_created = r
    ticket_meta[tid] = (source, info_tag, created)
    if comment and tc_created:
        ticket_comments[tid].append((comment, bool(is_internal), tc_created))

# For each ticket find: last agent reply, last customer reply, hours since last agent reply
pending_cx = []  # agent replied, cx silent 24+ hrs

for tid, (source, info_tag, created) in ticket_meta.items():
    comments = ticket_comments.get(tid, [])
    if not comments:
        continue

    last_agent_time = None
    last_agent_msg = ''
    last_cx_time = None

    for comment, is_internal, tc_created in comments:
        if is_internal:
            continue  # skip internal notes for this analysis
        if is_agent_reply(comment):
            if last_agent_time is None or tc_created > last_agent_time:
                last_agent_time = tc_created
                last_agent_msg = comment.strip()[:120]
        elif is_customer_message(comment):
            if last_cx_time is None or tc_created > last_cx_time:
                last_cx_time = tc_created

    if last_agent_time is None:
        continue  # no agent reply at all

    # Check: agent replied AFTER last customer message (or no cx message at all)
    cx_replied_after = last_cx_time and last_cx_time > last_agent_time
    if cx_replied_after:
        continue  # customer replied after agent — ticket still active

    # Hours since agent last replied
    hours_since_agent = (now - last_agent_time).total_seconds() / 3600

    if hours_since_agent >= 24:
        pending_cx.append({
            'id': tid,
            'source': source,
            'src_label': {'1':'App','7':'Social','8':'IO/WA','9':'Sage'}.get(source, source),
            'info_tag': info_tag or '',
            'created': created,
            'last_agent_time': last_agent_time,
            'hours_since': round(hours_since_agent, 1),
            'last_cx_time': last_cx_time,
            'agent_msg': last_agent_msg,
        })

pending_cx.sort(key=lambda x: x['hours_since'], reverse=True)

# Source breakdown
from collections import Counter
src_count = Counter(t['src_label'] for t in pending_cx)
info_count = Counter(t['info_tag'] for t in pending_cx)

print("=" * 78)
print(f"  PENDING CX REPLY > 24 HOURS — Open tickets where agent replied, cx went silent")
print(f"  Current time (UTC): {now.strftime('%d %b %Y %H:%M')}")
print("=" * 78)
print(f"\n  Total tickets: {len(pending_cx)}")
print(f"\n  By source:")
for src, cnt in src_count.most_common():
    print(f"    {src:<10} {cnt}")
print(f"\n  By info_tag:")
for tag, cnt in info_count.most_common():
    print(f"    {(tag or 'none'):<15} {cnt}")

print()
print("=" * 78)
print(f"  FULL LIST — Ticket ID | Source | Hours Waiting | Last Agent Message")
print("=" * 78)
print(f"\n  {'Ticket ID':<13} {'Src':<7} {'Hours':>6}  {'Last Agent Message'}")
print(f"  {'-'*74}")
for t in pending_cx:
    print(f"  #{t['id']:<12} {t['src_label']:<7} {t['hours_since']:>6}h  {t['agent_msg'][:50]}")

print()
print("=" * 78)
print(f"  IDs ONLY — {len(pending_cx)} tickets safe to AUTO-CLOSE (cx silent 24+ hrs)")
print("=" * 78)
ids = [str(t['id']) for t in pending_cx]
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
