# -*- coding: utf-8 -*-
# Find all cases where same customer opened an empty App ticket + a Sage ticket
# within 60 seconds of each other (one is blank, system tagged the wrong one as Repeated)
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

# Find pairs: same customer, tickets within 60s of each other,
# one from App (source=1) with no comments, one from Sage (source=9)
# Look at last 30 days, both open and closed
cur.execute("""
    SELECT
        t_app.id          AS app_ticket_id,
        t_sage.id         AS sage_ticket_id,
        t_app.customer_id,
        t_app.created     AS app_created,
        t_sage.created    AS sage_created,
        ROUND(EXTRACT(EPOCH FROM (t_sage.created - t_app.created))::numeric, 1) AS gap_seconds,
        t_app.status      AS app_status,
        t_sage.status     AS sage_status,
        t_app.info_tag    AS app_info_tag,
        t_sage.info_tag   AS sage_info_tag,
        t_sage.sageai_category_slug,
        comment_count.cnt AS sage_comment_count
    FROM support_ticket t_app
    JOIN support_ticket t_sage
        ON t_sage.customer_id = t_app.customer_id
        AND t_sage.source = '9'
        AND t_sage.created BETWEEN t_app.created AND t_app.created + INTERVAL '120 seconds'
        AND t_sage.id != t_app.id
    LEFT JOIN (
        SELECT ticket_id, COUNT(*) as cnt
        FROM support_ticketcomment
        WHERE is_internal = false
        GROUP BY ticket_id
    ) comment_count ON comment_count.ticket_id = t_sage.id
    WHERE t_app.source = '1'
      AND t_app.created >= NOW() - INTERVAL '30 days'
      AND NOT EXISTS (
          SELECT 1 FROM support_ticketcomment tc
          WHERE tc.ticket_id = t_app.id AND tc.is_internal = false
      )
    ORDER BY t_app.created DESC
""")
rows = cur.fetchall()

print("=" * 80)
print(f"  EMPTY APP TICKET + SAGE TICKET — SAME CUSTOMER WITHIN 2 MINUTES")
print(f"  Pattern: Customer opens blank App ticket, then raises issue on Sage seconds later")
print(f"  The empty App ticket should be auto-closed (no issue stated)")
print("=" * 80)
print(f"\n  Total pairs found: {len(rows)}\n")

open_app_tickets = []
closed_app_tickets = []

print(f"  {'App Ticket':<13} {'Sage Ticket':<13} {'Gap':>6}s  {'App Status':<10} {'Sage Tag':<10} {'Sage Category'}")
print(f"  {'-'*78}")

for row in rows:
    app_id, sage_id, cust_id, app_created, sage_created, gap, app_status, sage_status, app_tag, sage_tag, sage_cat, sage_cnt = row
    app_s = 'Open' if app_status == '1' else 'Closed'
    sage_s = 'Open' if sage_status == '1' else 'Closed'
    cat = (sage_cat or 'unknown')[:30]
    print(f"  #{app_id:<12} #{sage_id:<12} {gap:>6}s  {app_s:<10} {sage_tag or '-':<10} {cat}")
    if app_status == '1':
        open_app_tickets.append((app_id, sage_id, gap, cat))
    else:
        closed_app_tickets.append((app_id, sage_id, gap, cat))

print(f"\n  {'-'*78}")
print(f"  Empty App tickets still OPEN  : {len(open_app_tickets)}")
print(f"  Empty App tickets already closed: {len(closed_app_tickets)}")

print()
print("=" * 80)
print("  OPEN EMPTY APP TICKET IDs — Safe to AUTO-CLOSE immediately")
print("=" * 80)
if open_app_tickets:
    ids = [str(r[0]) for r in open_app_tickets]
    print(f"\n  Total: {len(ids)} tickets\n")
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))
else:
    print("\n  None — all already closed.")

print()
print("=" * 80)
print("  FULL PAIR LIST (App ticket ID | Sage ticket ID | Gap | Sage Category)")
print("=" * 80)
print(f"\n  {'#':<4} {'App Ticket':<13} {'Sage Ticket':<13} {'Gap':>6}s  Sage Category")
print(f"  {'-'*70}")
for i, row in enumerate(rows, 1):
    app_id, sage_id, cust_id, *_, gap, app_status, sage_status, app_tag, sage_tag, sage_cat, sage_cnt = row
    print(f"  {i:<4} #{app_id:<12} #{sage_id:<12} {gap:>6}s  {sage_cat or 'unknown'}")

cur.close()
conn.close()
