import psycopg2, json
from collections import defaultdict

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT t.id, t.source, t.info_tag, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
    AND t.created >= NOW() - INTERVAL '30 days'
    GROUP BY t.id, t.source, t.info_tag, t.created
    ORDER BY t.id DESC
""")
all_tickets = cur.fetchall()

def get_full_text(all_comments_raw):
    if not all_comments_raw:
        return ""
    full = []
    for c in all_comments_raw.split('|||'):
        try:
            data = json.loads(c)
            def walk(obj):
                if isinstance(obj, dict):
                    if obj.get('type') == 'Text' and obj.get('value'):
                        full.append(obj['value'].strip())
                    for v in obj.values():
                        if isinstance(v, (dict, list)):
                            walk(v)
                elif isinstance(obj, list):
                    for item in obj:
                        walk(item)
            walk(data)
        except Exception:
            if c and len(c) > 5:
                full.append(c[:300])
    return ' '.join(full).lower()

categories = defaultdict(list)

for row in all_tickets:
    tid, source, info_tag, created, all_comments = row
    text = get_full_text(all_comments)
    date_str = created.strftime('%d %b %Y')

    if info_tag == 'Repeated':
        categories['CAT1_REPEATED'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['lost something', 'found something', 'i lost', 'i found', 'forgot my',
                                 'left my', 'earphone', 'earpod', 'earbuds', 'rsa token',
                                 'left behind', 'left it on', 'seat 5b', 'seat 7d', 'seat 4d',
                                 'my bag', 'my wallet', 'my phone', 'umbrella', 'charger on seat']):
        categories['CAT7_LOST_FOUND'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['driving rashly', 'rash driving', 'behaving rudely', 'rude',
                                 'unprofessional', 'wrong route', 'unscheduled stop',
                                 'talking on phone', 'talking on call', 'honking',
                                 'driving very slowly', 'i have an issue with driver',
                                 'i had an issue with driver', 'other driver issue',
                                 'driver took', 'driver did not stop']):
        categories['CAT8_DRIVER'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['ac is not working', 'ac was not working', 'ac not working',
                                 'i have an issue with ac', 'i had an issue with ac',
                                 'increase the ac', 'decrease the ac', 'ac temperature',
                                 'ac was very cold', 'ac was very hot', 'ac vent',
                                 'ac is making noise', 'informed the driver to decrease',
                                 'no ac', 'no cool', 'heat inside']):
        categories['CAT3_AC'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['my seat has a problem', 'my seat had a problem',
                                 'slider is not working', 'slider was not working',
                                 'handrest', 'recliner', 'footrest', 'charging point',
                                 'bottle holder', 'seat pocket']):
        categories['CAT4_SEAT'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['bus quality and hygiene', 'bus is not clean', 'bus was not clean',
                                 'hygiene', 'dirty', 'flies in the bus', 'flies were',
                                 'water is dripping', 'water was dripping', 'water dripping',
                                 'bus broke down', 'bus making noise', 'noise due to the bus',
                                 'poor condition', 'smell', 'odour', 'stink']):
        categories['CAT5_HYGIENE'].append((tid, date_str, source))
        continue
    if any(k in text for k in ["where is my bus", "tracking issue", "tracking was wrong",
                                 "tracking is wrong", "cannot track", "can't track",
                                 "couldn't track", "bus is not moving", "bus was not moving",
                                 "unable to track", "i was unable to track", "i can't track"]):
        categories['CAT6_TRACKING'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['the bus was late', 'bus is late', 'bus was late',
                                 'bus left early', 'bus departed early', 'bus did not wait',
                                 'did not pick me', 'missed my bus', 'i missed my bus',
                                 'bus left before', 'bus skipped my stop', 'didn\'t stop at']):
        categories['CAT9_BUS_TIMING'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['suggestions: route', 'existing route', 'new route', 'new stop',
                                 'shuttles for', 'add a route', 'add route', 'start a route',
                                 'via dwarka', 'b2b', 'rentals', 'route suggestion',
                                 'timing suggestion', 'are there any buses', 'are there any shuttles']):
        categories['CAT2_SUGGESTION'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['payment', 'refund', 'wallet', 'invoice', 'amount deducted',
                                 'paid multiple', 'bank account', 'money deducted',
                                 'i have a payment', 'i had a payment', 'billing',
                                 'amount not reflected', 'not reflecting']):
        categories['CAT10_PAYMENT'].append((tid, date_str, source))
        continue
    if any(k in text for k in ['app issue', 'app not working', 'slow app', 'unable to book',
                                 'unable to cancel', 'unable to reschedule', 'boarding pass',
                                 'cannot book', "can't book", 'update the app', 'reinstall',
                                 'app is not', 'app was not']):
        categories['CAT11_APP'].append((tid, date_str, source))
        continue
    if source in ('8', '7'):
        categories['CAT12_MANUAL'].append((tid, date_str, source))
        continue
    categories['UNCATEGORIZED'].append((tid, date_str, source))

# ── PRINT SUMMARY ────────────────────────────────────────────────────────────
meta = {
    'CAT1_REPEATED':  ('Repeated Tickets',                  'AUTO-CLOSE'),
    'CAT2_SUGGESTION':('Route / Timing Suggestions',        'AUTO-CLOSE'),
    'CAT3_AC':        ('AC Issues',                         'AUTO-CLOSE'),
    'CAT4_SEAT':      ('Seat / Hardware Issues',            'AUTO-CLOSE'),
    'CAT5_HYGIENE':   ('Hygiene / Bus Condition',           'AUTO-CLOSE'),
    'CAT6_TRACKING':  ('Tracking Complaints',               'AUTO-CLOSE'),
    'CAT7_LOST_FOUND':('Lost & Found',                      'MANUAL    '),
    'CAT8_DRIVER':    ('Driver Behaviour Issues',           'MANUAL    '),
    'CAT9_BUS_TIMING':('Bus Timing (Late/Early/Missed)',    'REVIEW    '),
    'CAT10_PAYMENT':  ('Payment / Refund / Invoice',        'REVIEW    '),
    'CAT11_APP':      ('App / Booking Issues',              'REVIEW    '),
    'CAT12_MANUAL':   ('Manual Channel (WhatsApp/Other)',   'MANUAL    '),
    'UNCATEGORIZED':  ('Uncategorized / Unknown',           'REVIEW    '),
}
total = sum(len(v) for v in categories.values())

print(f"\n{'='*72}")
print(f"  OPEN TICKET BREAKDOWN  —  Total: {total} tickets (last 30 days)")
print(f"{'='*72}")
print(f"  {'#':<3} {'Category':<36} {'Action':<12} {'Count':>5}  {'%':>5}")
print(f"  {'-'*65}")
for i, (key, (label, action)) in enumerate(meta.items(), 1):
    count = len(categories[key])
    pct = count / total * 100
    print(f"  {i:<3} {label:<36} {action:<12} {count:>5}  {pct:>4.1f}%")
print(f"  {'-'*65}")
print(f"  {'':3} {'TOTAL':<36} {'':12} {total:>5}  100.0%")

auto = sum(len(categories[k]) for k in ['CAT1_REPEATED','CAT2_SUGGESTION','CAT3_AC','CAT4_SEAT','CAT5_HYGIENE','CAT6_TRACKING'])
review = sum(len(categories[k]) for k in ['CAT9_BUS_TIMING','CAT10_PAYMENT','CAT11_APP','UNCATEGORIZED'])
manual = sum(len(categories[k]) for k in ['CAT7_LOST_FOUND','CAT8_DRIVER','CAT12_MANUAL'])
print(f"\n  AUTO-CLOSE now  : {auto:>4} tickets ({auto/total*100:.1f}%)")
print(f"  REVIEW needed   : {review:>4} tickets ({review/total*100:.1f}%)")
print(f"  MANUAL required : {manual:>4} tickets ({manual/total*100:.1f}%)")
print(f"{'='*72}\n")

# ── PRINT IDs PER CATEGORY ───────────────────────────────────────────────────
for key, (label, action) in meta.items():
    tickets = categories[key]
    if not tickets:
        continue
    print(f"\n{'─'*72}")
    print(f"  {label.upper()}  [{action.strip()}]  —  {len(tickets)} tickets")
    print(f"{'─'*72}")
    # Print as comma-separated list of IDs
    ids = [str(t[0]) for t in tickets]
    # group into rows of 10
    for i in range(0, len(ids), 10):
        print("  " + "  ".join(f"#{x}" for x in ids[i:i+10]))

cur.close()
conn.close()
