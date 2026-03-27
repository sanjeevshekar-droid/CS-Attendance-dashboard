# -*- coding: utf-8 -*-
import sys, io, psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT
        t.id,
        t.status,
        t.info_tag,
        t.source,
        t.created,
        t.customer_id,
        t.creator_id,
        t.sageai_category_slug,
        t.sageai_subcategory_slug
    FROM support_ticket t
    WHERE t.id IN (12009605, 12009606)
    ORDER BY t.id
""")
rows = cur.fetchall()

print("=" * 72)
print("  TICKET COMPARISON: #12009605 vs #12009606")
print("=" * 72)

fields = ['ID', 'Status', 'info_tag', 'Source', 'Created', 'customer_id', 'creator_id', 'sage_category', 'sage_subcategory']
for row in rows:
    print()
    for f, v in zip(fields, row):
        print(f"  {f:<18}: {v}")

if len(rows) == 2:
    print()
    print("=" * 72)
    same_customer = rows[0][5] == rows[1][5]
    same_creator  = rows[0][6] == rows[1][6]
    print(f"  Same customer_id : {same_customer}  ({rows[0][5]} vs {rows[1][5]})")
    print(f"  Same creator_id  : {same_creator}  ({rows[0][6]} vs {rows[1][6]})")
    if same_customer or same_creator:
        print()
        print("  => CONFIRMED: Same customer, duplicate pair.")
        print("     #12009605 (App, no message) is the empty ticket.")
        print("     #12009606 (Sage, has Repeated flag) has actual suggestion content.")
        print("     The Repeated tag in DB should be on #12009605, not #12009606.")
    else:
        print()
        print("  => Different customers — not a duplicate pair.")
    print("=" * 72)

cur.close()
conn.close()
