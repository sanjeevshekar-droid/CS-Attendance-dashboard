# -*- coding: utf-8 -*-
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT t.id, t.status, t.info_tag, t.source, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.id IN (12009605, 12009606)
    GROUP BY t.id, t.status, t.info_tag, t.source, t.created
    ORDER BY t.id
""")
rows = cur.fetchall()

def get_customer_text(raw):
    msgs = []
    if not raw:
        return ''
    for c in raw.split('|||'):
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
    return ' '.join(msgs)

for row in rows:
    tid, status, info_tag, source, created, raw = row
    cust = get_customer_text(raw)
    status_str = 'Open' if status == '1' else 'Closed'
    print(f"Ticket #{tid}")
    print(f"  Status   : {status_str}")
    print(f"  info_tag : {info_tag}")
    print(f"  Source   : {source}")
    print(f"  Created  : {created.strftime('%d %b %Y %H:%M')}")
    print(f"  Customer text ({len(cust)} chars): '{cust[:200]}'")
    print(f"  Raw comments count: {len(raw.split('|||')) if raw else 0}")
    print()

cur.close()
conn.close()
