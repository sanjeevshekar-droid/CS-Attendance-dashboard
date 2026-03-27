# -*- coding: utf-8 -*-
# Get associate name who replied on the 40 safe auto-close dropped-off tickets
import sys, io, psycopg2, json
from collections import defaultdict
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

now = datetime.now(timezone.utc)

ids_40 = [
    12009814,12009777,12008689,12004012,12002471,12001127,11995643,11995601,
    11991714,11991183,11990392,11990115,11982066,11981754,11981181,11980711,
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

placeholders = ','.join(['%s'] * len(ids_40))

cur.execute(f"""
    SELECT
        tc.ticket_id,
        tc.comment,
        tc.is_internal,
        tc.created,
        tc.author_id,
        p.first_name,
        p.last_name,
        e.email
    FROM support_ticketcomment tc
    LEFT JOIN users_employee e ON e.id = tc.author_id
    LEFT JOIN users_person p ON p.user_id = e.person_id
    WHERE tc.ticket_id IN ({placeholders})
    ORDER BY tc.ticket_id, tc.created ASC
""", ids_40)
rows = cur.fetchall()

ticket_comments = defaultdict(list)
for r in rows:
    tid, comment, is_internal, tc_created, author_id, first, last, email = r
    name = f"{first or ''} {last or ''}".strip() or (email or f'ID:{author_id}')
    ticket_comments[tid].append((comment, bool(is_internal), tc_created, name))

print("=" * 80)
print(f"  40 SAFE AUTO-CLOSE TICKETS — ASSOCIATE WHO LAST REPLIED")
print(f"  UTC now: {now.strftime('%d %b %Y %H:%M')}")
print("=" * 80)
print(f"\n  {'#':<4} {'Ticket ID':<13} {'Hours Ago':>9}  {'Associate':<22} Last Message Snippet")
print(f"  {'-'*78}")

from collections import Counter
assoc_count = Counter()

for i, tid in enumerate(sorted(ids_40, reverse=True), 1):
    comments = ticket_comments.get(tid, [])

    last_agent_time = None
    last_agent_name = 'Unknown'
    last_agent_msg  = ''

    for comment, is_internal, tc_created, name in comments:
        if is_internal:
            continue
        if is_agent_reply(comment):
            if not last_agent_time or tc_created > last_agent_time:
                last_agent_time = tc_created
                last_agent_name = name
                last_agent_msg  = comment.strip()[:50]

    if last_agent_time:
        hours = round((now - last_agent_time).total_seconds() / 3600, 1)
        assoc_count[last_agent_name] += 1
    else:
        hours = 0
        last_agent_name = '(no agent reply)'

    print(f"  {i:<4} #{tid:<12} {str(hours)+'h':>9}  {last_agent_name:<22} {last_agent_msg[:35]}")

print()
print("=" * 80)
print("  ASSOCIATE BREAKDOWN")
print("=" * 80)
print(f"\n  {'Associate':<25} {'Tickets':>7}")
print(f"  {'-'*35}")
for name, cnt in assoc_count.most_common():
    print(f"  {name:<25} {cnt:>7}")

cur.close()
conn.close()
