# -*- coding: utf-8 -*-
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

# Get one AC ticket marked as Repeated, and find an earlier ticket from the same customer
cur.execute("""
    SELECT t.id, t.status, t.info_tag, t.created, t.customer_id,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.id = 12009713
    GROUP BY t.id, t.status, t.info_tag, t.created, t.customer_id
""")
row = cur.fetchone()
tid, status, info_tag, created, customer_id, raw = row

def get_customer_text(raw):
    msgs = []
    if not raw:
        return []
    for c in (raw or '').split('|||'):
        try:
            data = json.loads(c)
            def walk(obj, in_yellow=False):
                if isinstance(obj, dict):
                    yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                    if obj.get('type') == 'Text' and obj.get('value') and yellow:
                        v = obj['value'].strip()
                        if len(v) > 2:
                            msgs.append(v)
                    for v in obj.values():
                        if isinstance(v, (dict, list)):
                            walk(v, yellow)
                elif isinstance(obj, list):
                    for item in obj:
                        walk(item, in_yellow)
            walk(data)
        except Exception:
            pass
    return msgs

msgs = get_customer_text(raw)

print("=" * 72)
print("  EXAMPLE: AC ISSUE category — Repeated ticket")
print("=" * 72)
print(f"\n  Ticket #12009713")
print(f"  Status    : {'Open' if status == '1' else 'Closed'}")
print(f"  info_tag  : {info_tag}   <-- This is what 'Repeated' means in the table")
print(f"  Created   : {created.strftime('%d %b %Y %H:%M')}")
print(f"  Customer ID: {customer_id}")
print(f"\n  What the customer selected/typed in Sage:")
for m in msgs:
    print(f"    > {m}")

# Now find the earlier ticket from same customer
cur.execute("""
    SELECT t.id, t.status, t.created, t.info_tag,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.customer_id = %s
      AND t.id != 12009713
      AND t.created < %s
    GROUP BY t.id, t.status, t.created, t.info_tag
    ORDER BY t.created DESC
    LIMIT 3
""", (customer_id, created))
prev_rows = cur.fetchall()

print(f"\n  Why is it marked 'Repeated'?")
print(f"  This same customer (ID: {customer_id}) had raised a similar ticket before:")
print()
for pr in prev_rows:
    p_id, p_status, p_created, p_tag, p_raw = pr
    p_msgs = get_customer_text(p_raw)
    p_status_str = 'Open' if p_status == '1' else 'Closed/Resolved'
    print(f"  Previous ticket #{p_id}")
    print(f"    Status  : {p_status_str}")
    print(f"    Created : {p_created.strftime('%d %b %Y %H:%M')}")
    print(f"    Customer said:")
    for m in p_msgs[:5]:
        print(f"      > {m}")
    print()

print("=" * 72)
print("  SUMMARY OF WHAT THE TABLE COLUMNS MEAN:")
print("=" * 72)
print("""
  Category  : AC Issue
  Count     : 63  --> Total number of open AC-related tickets raised in last 30 days
  Repeated  : 44  --> Out of those 63 tickets, 44 are from customers who had
                      already raised the SAME issue before (info_tag = 'Repeated')
                      meaning the first ticket was not resolved to their satisfaction
                      and they came back to complain again.

  The remaining 63 - 44 = 19 are first-time AC complaints.

  All 63 are AUTO-CLOSE because AC issues are resolved on the bus —
  Sage can acknowledge, log it for ops, and close without CS intervention.
""")

cur.close()
conn.close()
