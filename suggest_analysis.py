# -*- coding: utf-8 -*-
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

def extract_customer_text(comment_str):
    texts = []
    try:
        data = json.loads(comment_str)
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                bg = obj.get('background', '')
                yellow = bg == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    v = obj['value'].strip()
                    if len(v) > 3:
                        texts.append(v)
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, in_yellow)
        walk(data)
    except Exception:
        pass
    return texts

def extract_agent_response(comment_str):
    try:
        json.loads(comment_str)
        return None
    except Exception:
        if comment_str and len(comment_str) > 10:
            return comment_str[:300]
    return None

# Q1: What do CS agents actually do with resolved suggestion tickets?
print("=" * 72)
print("Q1: HOW DO CS AGENTS RESPOND TO RESOLVED SUGGESTION TICKETS?")
print("=" * 72)

cur.execute("""
    SELECT t.id, t.status, t.modified,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '2'
    AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND EXISTS (
        SELECT 1 FROM support_ticketcomment tc2
        WHERE tc2.ticket_id = t.id
        AND tc2.comment LIKE '%%Suggestions: Route%%'
    )
    GROUP BY t.id, t.status, t.modified
    ORDER BY t.modified DESC
    LIMIT 20
""")
rows = cur.fetchall()
agent_responses_found = 0
for row in rows:
    tid, status, modified, raw = row
    agent_msgs = []
    for c in (raw or '').split('|||'):
        resp = extract_agent_response(c)
        if resp:
            agent_msgs.append(resp)
    if agent_msgs:
        agent_responses_found += 1
        print(f"\n  Ticket #{tid} (closed {modified.strftime('%d %b')})")
        for m in agent_msgs[:2]:
            print(f"    CS: \"{m[:180]}\"")

if agent_responses_found == 0:
    print(f"\n  Checked {len(rows)} closed suggestion tickets.")
    print("  Result: CS agents gave ZERO typed responses to any of them.")
    print("  These tickets are being bulk-closed with NO agent action.")
    print("  => Auto-close is completely safe for these.")

# Q2: Average time suggestion tickets sit open before being closed
print()
print("=" * 72)
print("Q2: HOW LONG DO SUGGESTION TICKETS SIT OPEN?")
print("=" * 72)

cur.execute("""
    SELECT
        ROUND(AVG(EXTRACT(EPOCH FROM (t.modified - t.created))/3600)::numeric, 1) as avg_h,
        ROUND(MIN(EXTRACT(EPOCH FROM (t.modified - t.created))/3600)::numeric, 1) as min_h,
        ROUND(MAX(EXTRACT(EPOCH FROM (t.modified - t.created))/3600)::numeric, 1) as max_h,
        COUNT(*) as cnt
    FROM support_ticket t
    WHERE t.status = '2'
    AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND EXISTS (
        SELECT 1 FROM support_ticketcomment tc2
        WHERE tc2.ticket_id = t.id
        AND tc2.comment LIKE '%%Suggestions: Route%%'
    )
""")
r = cur.fetchone()
if r and r[0]:
    print(f"\n  Sample: {r[3]} recently closed suggestion tickets")
    print(f"  Average time open : {r[0]} hours")
    print(f"  Shortest          : {r[1]} hours")
    print(f"  Longest           : {r[2]} hours")
    print(f"  => With auto-close: 0 hours. Saves {r[0]}h avg wait per ticket.")

# Q3: Genuine suggestion vs wrong-menu tickets
print()
print("=" * 72)
print("Q3: GENUINE SUGGESTIONS vs WRONG-MENU (real CS issue) BREAKDOWN")
print("=" * 72)

cur.execute("""
    SELECT t.id,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND EXISTS (
        SELECT 1 FROM support_ticketcomment tc2
        WHERE tc2.ticket_id = t.id
        AND (tc2.comment LIKE '%%Suggestions: Route%%'
             OR tc2.comment LIKE '%%can you start%%'
             OR tc2.comment LIKE '%%shuttles for%%'
             OR tc2.comment LIKE '%%new route%%')
    )
    GROUP BY t.id
    ORDER BY t.id DESC
""")
rows = cur.fetchall()

ISSUE_KW = ['refund','cancel','payment','deduct','wallet','reschedule',
            'not working','app issue','booking','recliner','ac issue',
            'driver','delay','lost','found','invoice','overcharge',
            'not allow','cannot reschedule','not able to reschedule',
            'charged','amount','seat booked','ride cancelled']

genuine = []
wrong_menu = []
dropped_off = []

for row in rows:
    tid, raw = row
    all_cust = []
    for c in (raw or '').split('|||'):
        all_cust.extend(extract_customer_text(c))
    cust_text = ' '.join(all_cust).lower()
    skip_words = {'am','pm','suggestions:','route,','timing,','stop','existing','other',
                  'subscription','referral','suggestions','safety'}
    cust_filtered = ' '.join(w for w in cust_text.split() if w not in skip_words)

    if len(cust_filtered.strip()) < 12:
        dropped_off.append(tid)
    elif any(k in cust_filtered for k in ISSUE_KW):
        wrong_menu.append((tid, cust_text[:100]))
    else:
        genuine.append((tid, cust_text[:100]))

total = len(genuine) + len(wrong_menu) + len(dropped_off)
print(f"\n  Total open tickets in Suggestions category: {total}")
print()
print(f"  1. Genuine route/stop/timing suggestions: {len(genuine)} ({len(genuine)/total*100:.0f}%)")
print(f"     => AUTO-CLOSE immediately after acknowledgement")
print()
print(f"  2. Menu-only (customer dropped off):       {len(dropped_off)} ({len(dropped_off)/total*100:.0f}%)")
print(f"     => AUTO-CLOSE immediately (no issue raised)")
print()
print(f"  3. Wrong-menu (real CS issue):             {len(wrong_menu)} ({len(wrong_menu)/total*100:.0f}%)")
print(f"     => Re-route to correct category, keep open for CS")

print()
print("  Sample genuine suggestions:")
for tid, txt in genuine[:10]:
    print(f"    #{tid}: {txt[:100]}")

print()
print("  Sample wrong-menu tickets (SHOULD NOT be auto-closed):")
for tid, txt in wrong_menu[:10]:
    print(f"    #{tid}: {txt[:100]}")

cur.close()
conn.close()
