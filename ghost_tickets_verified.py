# -*- coding: utf-8 -*-
# Refined ghost ticket finder: only include pairs where the Sage ticket
# has an actual sageai_category_slug (customer went through at least one Sage menu step)
import sys, io, psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT ON (t_app.id)
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
        AND t_sage.sageai_category_slug IS NOT NULL
    WHERE t_app.source = '1'
      AND t_app.status = '1'
      AND t_app.created >= NOW() - INTERVAL '30 days'
      AND NOT EXISTS (
          SELECT 1 FROM support_ticketcomment tc
          WHERE tc.ticket_id = t_app.id AND tc.is_internal = false
      )
    ORDER BY t_app.id DESC, t_sage.created ASC
""")
rows = cur.fetchall()

print("=" * 80)
print("  VERIFIED GHOST TICKETS — Empty App ticket + Sage ticket with actual category")
print("  (Excludes cases where Sage ticket also had no category / was abandoned)")
print("=" * 80)
print(f"\n  Total verified open ghost tickets: {len(rows)}\n")

ids = [str(r[0]) for r in rows]
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

print()
print("=" * 80)
print(f"  {'#':<4} {'App Ticket':<13} {'Date':<13} {'Gap':>6}s  {'Sage Ticket':<13} Sage Category")
print(f"  {'-'*76}")
for i, row in enumerate(rows, 1):
    app_id, app_status, app_created, sage_id, sage_cat, gap = row
    date = app_created.strftime('%d %b %Y')
    cat = (sage_cat or 'unknown')[:30]
    print(f"  {i:<4} #{app_id:<12} {date:<13} {gap:>6}s  #{sage_id:<12} {cat}")

from collections import Counter
cat_count = Counter(r[4] for r in rows)
print()
print("=" * 80)
print("  SAGE CATEGORY BREAKDOWN")
print("=" * 80)
print()
for cat, cnt in cat_count.most_common():
    print(f"  {cat:<42} {cnt:>3} tickets")

cur.close()
conn.close()
