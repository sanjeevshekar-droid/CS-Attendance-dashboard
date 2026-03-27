# -*- coding: utf-8 -*-
# Correctly identify Lost & Found tickets among repeated ones
# Key fix: check customer-selected messages ONLY (yellow #FFEEC0 bubbles)
# not the full text (which includes Sage's displayed menu options for every ticket)
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT t.id, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
    AND t.info_tag = 'Repeated'
    AND t.created >= NOW() - INTERVAL '30 days'
    GROUP BY t.id, t.created
    ORDER BY t.created DESC
""")
rows = cur.fetchall()
print(f"Total repeated open tickets: {len(rows)}\n")

def get_customer_text(raw):
    """Extract ONLY text from yellow (#FFEEC0) customer bubbles — not Sage menu displays."""
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
            # Plain text comment (agent reply) — not a customer Sage message
            pass
    return ' '.join(msgs).lower()

# Sage Lost & Found flow: customer selects these exact options
# These appear ONLY in customer yellow bubbles when customer chooses L&F path
LNF_CUSTOMER_SIGNALS = [
    'i lost or found something',  # customer taps this category
    'i lost something',           # customer taps this subcategory
    'i found something',          # customer taps this subcategory
]

# Additional free-text from customer describing a lost item
ITEM_KEYWORDS = [
    'earphone', 'earpod', 'earbuds', 'airpod',
    'umbrella', 'charger', 'laptop bag', 'water bottle',
    'lunch box', 'lunchbox', 'tiffin',
    'left my bag', 'forgot my bag', 'lost my bag',
    'left my phone', 'forgot my phone', 'lost my phone',
    'lost my wallet', 'forgot my wallet', 'left my wallet',
    'left my charger', 'forgot my charger',
    'left my keys', 'forgot my keys', 'lost my keys',
    'left my glasses', 'forgot my glasses',
    'left my umbrella', 'forgot my umbrella',
    'rsa token', 'lost one item', 'foke', 'pouch',
    'id card on seat', 'watch on seat',
    'left it on the bus', 'left on the bus',
    'left behind on seat', 'forgot on seat',
    'forgot on bus', 'left in the bus',
]

lost_found = []
other = []

for row in rows:
    tid, created, raw = row
    cust = get_customer_text(raw)

    is_lnf = (
        any(sig in cust for sig in LNF_CUSTOMER_SIGNALS) or
        any(k in cust for k in ITEM_KEYWORDS)
    )

    # Build readable snippet from customer text
    snippet = ''
    for chunk in cust.split('.'):
        s = chunk.strip()
        ignore_words = ['sage', 'cityflo support assistant', 'please choose',
                        'hey there', 'what can i assist']
        if len(s) > 10 and not any(x in s for x in ignore_words):
            snippet = s[:85]
            break
    if not snippet:
        snippet = cust[:85] if len(cust) > 5 else '(no customer text)'

    if is_lnf:
        lost_found.append((tid, created, snippet))
    else:
        other.append((tid, created, snippet))

print("=" * 80)
print(f"  LOST & FOUND — REPEATED TICKETS  ({len(lost_found)} confirmed)")
print(f"  All MANUAL: customer's item still missing, needs driver/ops coordination")
print("=" * 80)
print()
print(f"  {'#':<4} {'Ticket ID':<13} {'Date':<13} Customer's Actual Message")
print(f"  {'-'*77}")
for i, (tid, created, snippet) in enumerate(lost_found, 1):
    print(f"  {i:<4} #{tid:<12} {created.strftime('%d %b %Y'):<13} {snippet}")

print()
print("=" * 80)
print("  TICKET IDs ONLY:")
print("=" * 80)
ids = [str(r[0]) for r in lost_found]
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

print()
print(f"\n  (Remaining {len(other)} repeated tickets are other issue types — not L&F)")

cur.close()
conn.close()
