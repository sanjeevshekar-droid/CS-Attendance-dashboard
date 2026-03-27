# -*- coding: utf-8 -*-
# Find all empty App tickets that are duplicates of a Sage ticket from the same customer
# Focus: unique App ticket IDs only (deduplicated), open ones for auto-close
import sys, io, psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT
        t_app.id          AS app_ticket_id,
        t_app.status      AS app_status,
        t_app.created     AS app_created,
        t_sage.id         AS sage_ticket_id,
        t_sage.sageai_category_slug,
        ROUND(EXTRACT(EPOCH FROM (t_sage.created - t_app.created))::numeric, 1) AS gap_seconds
    FROM support_ticket t_app
    JOIN support_ticket t_sage
        ON t_sage.customer_id = t_app.customer_id
        AND t_sage.source = '9'
        AND t_sage.created BETWEEN t_app.created AND t_app.created + INTERVAL '120 seconds'
        AND t_sage.id != t_app.id
    WHERE t_app.source = '1'
      AND t_app.created >= NOW() - INTERVAL '30 days'
      AND NOT EXISTS (
          SELECT 1 FROM support_ticketcomment tc
          WHERE tc.ticket_id = t_app.id AND tc.is_internal = false
      )
    ORDER BY t_app.id DESC
""")
rows = cur.fetchall()

# Deduplicate: if one App ticket matched multiple Sage tickets, keep only the first/closest
seen_app = {}
for row in rows:
    app_id, app_status, app_created, sage_id, sage_cat, gap = row
    if app_id not in seen_app:
        seen_app[app_id] = row

all_pairs   = list(seen_app.values())
open_pairs  = [r for r in all_pairs if r[1] == '1']
closed_pairs= [r for r in all_pairs if r[1] != '1']

print("=" * 80)
print("  GHOST APP TICKETS — Empty ticket opened seconds before a Sage conversation")
print("  Same customer, App ticket has zero comments, Sage ticket has the actual issue")
print("  Root cause: App triggers a blank ticket when customer switches to Sage")
print("=" * 80)
print(f"\n  Total unique empty App tickets found : {len(all_pairs)}")
print(f"  Still OPEN (need auto-close)         : {len(open_pairs)}")
print(f"  Already closed                       : {len(closed_pairs)}")

# --- OPEN TICKETS: ID list for team ---
print()
print("=" * 80)
print(f"  OPEN GHOST TICKETS TO AUTO-CLOSE  ({len(open_pairs)} tickets)")
print("=" * 80)
print()
open_ids = [str(r[0]) for r in open_pairs]
for i in range(0, len(open_ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in open_ids[i:i+8]))

# --- FULL TABLE ---
print()
print("=" * 80)
print("  FULL LIST — App Ticket | Date | Gap | Sage Ticket | Sage Category | Status")
print("=" * 80)
print(f"\n  {'#':<4} {'App ID':<13} {'Date':<13} {'Gap':>6}s  {'Sage ID':<13} {'Status':<8} Sage Category")
print(f"  {'-'*78}")
for i, row in enumerate(all_pairs, 1):
    app_id, app_status, app_created, sage_id, sage_cat, gap = row
    status = 'Open' if app_status == '1' else 'Closed'
    cat = (sage_cat or 'unknown')[:28]
    date = app_created.strftime('%d %b %Y')
    print(f"  {i:<4} #{app_id:<12} {date:<13} {gap:>6}s  #{sage_id:<12} {status:<8} {cat}")

# --- CATEGORY BREAKDOWN of the Sage tickets they paired with ---
print()
print("=" * 80)
print("  WHAT ISSUES WERE THESE CUSTOMERS ACTUALLY RAISING? (Sage category breakdown)")
print("=" * 80)
from collections import Counter
cat_count = Counter(r[4] or 'unknown' for r in open_pairs)
print()
for cat, cnt in cat_count.most_common():
    print(f"  {cat:<40} {cnt:>4} tickets")

cur.close()
conn.close()
