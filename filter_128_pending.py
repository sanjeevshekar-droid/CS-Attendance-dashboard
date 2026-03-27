# -*- coding: utf-8 -*-
# From the 128 clean dropped-off tickets, filter those with:
# 1. No comments at all (truly empty — safe to close)
# 2. Agent replied but cx hasn't responded in 24+ hours (safe to close)
import sys, io, psycopg2, json
from collections import defaultdict
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

now = datetime.now(timezone.utc)

ids_128 = [
    12009814,12009794,12009777,12009031,12008689,12006708,12006680,12006509,
    12006250,12006025,12005730,12005727,12004089,12004012,12003862,12003728,
    12003713,12003642,12003584,12003566,12003561,12003478,12003456,12003452,
    12003396,12003272,12003126,12002833,12002650,12002537,12002471,12002268,
    12002215,12002192,12001594,12001478,12001473,12001223,12001127,12001054,
    12000104,11999247,11999236,11999227,11999080,11998739,11998737,11998655,
    11998549,11998393,11998386,11998132,11997856,11997831,11997811,11997754,
    11997596,11996991,11996844,11996677,11996571,11996481,11996464,11996158,
    11995643,11995601,11995226,11995032,11995023,11994998,11994956,11994669,
    11993848,11992996,11992765,11992646,11992631,11992496,11992485,11992414,
    11992348,11992124,11991983,11991714,11991605,11991183,11990392,11990342,
    11990115,11989402,11989121,11988919,11988694,11988157,11987968,11987335,
    11987165,11986873,11986321,11983757,11982066,11981754,11981181,11980711,
    11980289,11980099,11980073,11979200,11979181,11978971,11978930,11978404,
    11977917,11977536,11977520,11976805,11976561,11976544,11976507,11976318,
    11976232,11976084,11975485,11975307,11975212,11974910,11974166,11967502
]

AGENT_PHRASES = [
    'good morning', 'good afternoon', 'good evening',
    'we apologize', 'sincerely apologize', 'i apologize',
    'we have noted', 'highlighted the issue', 'allow us some time',
    'we will get back', 'we will check', 'we will look into',
    'we have shared', 'we have escalated', 'i have escalated',
    'i have highlighted', 'i have addressed', 'rest assured',
    'thank you for', 'thanks for reaching', 'kindly allow',
    'please allow', 'we will resolve', 'we will update you',
    'i am sorry to hear', 'sorry to hear', 'i am writing to follow',
    'we kindly request you to please elaborate',
]

def is_agent_reply(comment):
    if not comment: return False
    try:
        json.loads(comment)
        return False
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
                    if isinstance(v, (dict, list)):
                        walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj: walk(item, in_yellow)
        walk(data)
        return len(' '.join(msgs).strip()) > 5
    except Exception:
        return not is_agent_reply(comment) and len(comment.strip()) > 8

placeholders = ','.join(['%s'] * len(ids_128))
cur.execute(f"""
    SELECT t.id, t.source, t.created,
           tc.comment, tc.is_internal, tc.created as tc_created
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.id IN ({placeholders})
    ORDER BY t.id, tc.created ASC
""", ids_128)
rows = cur.fetchall()

ticket_comments = defaultdict(list)
ticket_meta = {}
for r in rows:
    tid, source, created, comment, is_internal, tc_created = r
    ticket_meta[tid] = (source, created)
    if comment and tc_created:
        ticket_comments[tid].append((comment, bool(is_internal), tc_created))

no_comments     = []   # truly empty — safe
agent_replied_cx_silent = []  # agent replied, cx silent 24+ hrs — safe
agent_replied_recent    = []  # agent replied < 24hrs ago — keep open
cx_last_replied         = []  # cx replied last — keep open

for tid in ids_128:
    source, created = ticket_meta.get(tid, ('?', None))
    comments = ticket_comments.get(tid, [])
    src = {'1':'App','7':'Social','8':'IO/WA','9':'Sage'}.get(source, source)

    if not comments:
        no_comments.append((tid, src, created))
        continue

    last_agent_time = None
    last_agent_msg  = ''
    last_cx_time    = None

    for comment, is_internal, tc_created in comments:
        if is_internal:
            continue
        if is_agent_reply(comment):
            if not last_agent_time or tc_created > last_agent_time:
                last_agent_time = tc_created
                last_agent_msg  = comment.strip()[:100]
        elif is_customer_msg(comment):
            if not last_cx_time or tc_created > last_cx_time:
                last_cx_time = tc_created

    if last_agent_time is None:
        # Only internal comments, no public agent reply — treat as empty
        no_comments.append((tid, src, created))
        continue

    if last_cx_time and last_cx_time > last_agent_time:
        cx_last_replied.append((tid, src, created, last_cx_time))
        continue

    hours = (now - last_agent_time).total_seconds() / 3600
    if hours >= 24:
        agent_replied_cx_silent.append((tid, src, created, round(hours,1), last_agent_msg))
    else:
        agent_replied_recent.append((tid, src, created, round(hours,1), last_agent_msg))

safe = no_comments + agent_replied_cx_silent

print("=" * 72)
print(f"  FILTER 128 DROPPED-OFF — PENDING CX REPLY > 24 HRS")
print(f"  UTC now: {now.strftime('%d %b %Y %H:%M')}")
print("=" * 72)
print(f"\n  Truly empty (no comments)                    : {len(no_comments)}")
print(f"  Agent replied, cx silent 24+ hrs             : {len(agent_replied_cx_silent)}")
print(f"  Agent replied < 24 hrs ago (keep open)       : {len(agent_replied_recent)}")
print(f"  Cx replied last (keep open)                  : {len(cx_last_replied)}")
print(f"\n  SAFE TO AUTO-CLOSE                           : {len(safe)}")
print(f"  Keep open for now                            : {len(agent_replied_recent) + len(cx_last_replied)}")

if agent_replied_recent:
    print()
    print("=" * 72)
    print(f"  KEEP OPEN — Agent replied < 24 hrs ago ({len(agent_replied_recent)} tickets)")
    print("=" * 72)
    print(f"\n  {'Ticket ID':<13} {'Src':<7} {'Hrs ago':>7}  Last Agent Message")
    print(f"  {'-'*70}")
    for tid, src, created, hrs, msg in agent_replied_recent:
        print(f"  #{tid:<12} {src:<7} {hrs:>6}h  {msg[:50]}")

if cx_last_replied:
    print()
    print("=" * 72)
    print(f"  KEEP OPEN — Cx replied last ({len(cx_last_replied)} tickets)")
    print("=" * 72)
    for tid, src, created, cx_time in cx_last_replied:
        print(f"  #{tid:<12} {src:<7} cx last replied: {cx_time.strftime('%d %b %H:%M')}")

print()
print("=" * 72)
print(f"  FINAL SAFE AUTO-CLOSE LIST — {len(safe)} tickets")
print("=" * 72)
ids = sorted([r[0] for r in safe], reverse=True)
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
