# -*- coding: utf-8 -*-
# Re-examine all 685 "repeated" tickets — classify by actual issue type
import sys, io, psycopg2, json
from collections import defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

# Fetch all open repeated tickets with their full conversation
cur.execute("""
    SELECT t.id, t.source, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
    AND t.info_tag = 'Repeated'
    AND t.created >= NOW() - INTERVAL '30 days'
    GROUP BY t.id, t.source, t.created
    ORDER BY t.id DESC
""")
rows = cur.fetchall()
print(f"Total repeated open tickets fetched: {len(rows)}\n")

def get_text(raw):
    if not raw:
        return ''
    full = []
    for c in raw.split('|||'):
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
            if c:
                full.append(c[:300])
    return ' '.join(full).lower()

# Issue-type keyword sets — order matters (most specific first)
LOST_KW    = ['lost something', 'found something', 'i lost something', 'i found something',
               'forgot my', 'left my', 'earphone', 'earpod', 'earbuds', 'rsa token',
               'left behind', 'umbrella', 'my bag', 'my phone', 'lost one item',
               'lost my', 'found one', 'foke', 'fork', 'pouch', 'keys', 'glasses',
               'lost and found', 'i lost or found']

DRIVER_KW  = ['driving rashly', 'rash driving', 'behaving rudely', 'rude driver',
               'wrong route', 'unscheduled stop', 'talking on phone', 'talking on call',
               'honking', 'driving very slowly', 'issue with driver', 'driver was',
               'other driver issue', 'driver took', 'driver did not stop', 'driver misbehaved']

PAYMENT_KW = ['refund', 'wallet balance', 'amount deducted', 'paid multiple',
               'bank account', 'money deducted', 'payment issue', 'billing',
               'not reflecting', 'invoice', 'overcharge', 'amount not refund',
               'deducted from', 'charged twice', 'double charge', 'credits']

AC_KW      = ['ac is not working', 'ac was not working', 'issue with ac', 'ac not working',
               'increase the ac', 'decrease the ac', 'ac temperature', 'too cold', 'very cold',
               'ac was very cold', 'ac was very hot', 'too hot inside', 'ac vent',
               'ac is making noise', 'no ac', 'no cooling', 'ac off', 'ac problem']

SEAT_KW    = ['seat has a problem', 'seat had a problem', 'slider is not working',
               'slider was not working', 'handrest', 'recliner', 'footrest',
               'charging point', 'bottle holder', 'seat pocket', 'seat broken',
               'seat issue', 'broken seat']

HYGIENE_KW = ['not clean', 'bus was not clean', 'hygiene', 'dirty', 'flies in the bus',
               'water is dripping', 'water was dripping', 'bus broke down', 'bus breakdown',
               'bus making noise', 'poor condition', 'smell bad', 'bad smell', 'odour']

TRACKING_KW= ['where is my bus', 'tracking issue', 'tracking was wrong', 'tracking wrong',
               'cannot track', 'bus is not moving', 'bus was not moving',
               'unable to track', 'i was unable to track', 'bus not visible on map',
               'location not updating']

TIMING_KW  = ['bus was late', 'bus is late', 'bus left early', 'bus departed early',
               'bus did not wait', 'did not pick me', 'missed my bus', 'bus skipped',
               'bus did not come', 'no bus', 'bus was not there', 'bus left before']

SUGGEST_KW = ['suggestions: route', 'new route', 'add a route', 'start a route',
               'shuttles for', 'are there any bus', 'add a stop', 'new stop',
               'route from', 'route to', 'can you start', 'timing suggestion']

APP_KW     = ['app issue', 'app not working', 'slow app', 'unable to book',
               'unable to cancel', 'unable to reschedule', 'boarding pass',
               'cannot book', 'update the app', 'reinstall', 'login issue']

# Result buckets — key = actual issue type, value = list of ticket IDs
# Disposition: AUTO-CLOSE or MANUAL/REVIEW
issue_cats = {
    'LOST_FOUND':    {'label': 'Lost & Found',                 'disposition': 'MANUAL',     'tickets': []},
    'DRIVER':        {'label': 'Driver Behaviour Issues',       'disposition': 'MANUAL',     'tickets': []},
    'PAYMENT':       {'label': 'Payment / Refund / Invoice',    'disposition': 'REVIEW',     'tickets': []},
    'AC':            {'label': 'AC Issues',                     'disposition': 'AUTO-CLOSE', 'tickets': []},
    'SEAT':          {'label': 'Seat / Hardware Issues',        'disposition': 'AUTO-CLOSE', 'tickets': []},
    'HYGIENE':       {'label': 'Hygiene / Bus Condition',       'disposition': 'AUTO-CLOSE', 'tickets': []},
    'TRACKING':      {'label': 'Tracking Complaints',           'disposition': 'AUTO-CLOSE', 'tickets': []},
    'BUS_TIMING':    {'label': 'Bus Timing (Late/Early)',       'disposition': 'REVIEW',     'tickets': []},
    'SUGGESTION':    {'label': 'Route / Timing Suggestions',    'disposition': 'AUTO-CLOSE', 'tickets': []},
    'APP':           {'label': 'App / Booking Issues',          'disposition': 'REVIEW',     'tickets': []},
    'MENU_ONLY':     {'label': 'Menu-only (no issue text)',     'disposition': 'AUTO-CLOSE', 'tickets': []},
    'UNKNOWN':       {'label': 'Uncategorized',                 'disposition': 'REVIEW',     'tickets': []},
}

for row in rows:
    tid, src, created, raw = row
    text = get_text(raw)
    d = created.strftime('%d %b')

    def match(kws):
        return any(k in text for k in kws)

    if match(LOST_KW):
        issue_cats['LOST_FOUND']['tickets'].append((tid, d))
    elif match(DRIVER_KW):
        issue_cats['DRIVER']['tickets'].append((tid, d))
    elif match(PAYMENT_KW):
        issue_cats['PAYMENT']['tickets'].append((tid, d))
    elif match(AC_KW):
        issue_cats['AC']['tickets'].append((tid, d))
    elif match(SEAT_KW):
        issue_cats['SEAT']['tickets'].append((tid, d))
    elif match(HYGIENE_KW):
        issue_cats['HYGIENE']['tickets'].append((tid, d))
    elif match(TRACKING_KW):
        issue_cats['TRACKING']['tickets'].append((tid, d))
    elif match(TIMING_KW):
        issue_cats['BUS_TIMING']['tickets'].append((tid, d))
    elif match(SUGGEST_KW):
        issue_cats['SUGGESTION']['tickets'].append((tid, d))
    elif match(APP_KW):
        issue_cats['APP']['tickets'].append((tid, d))
    elif len(text.strip()) < 30:
        issue_cats['MENU_ONLY']['tickets'].append((tid, d))
    else:
        issue_cats['UNKNOWN']['tickets'].append((tid, d))

# --- PRINT SUMMARY ---
total = sum(len(v['tickets']) for v in issue_cats.values())
auto  = sum(len(v['tickets']) for v in issue_cats.values() if v['disposition'] == 'AUTO-CLOSE')
manual= sum(len(v['tickets']) for v in issue_cats.values() if v['disposition'] == 'MANUAL')
review= sum(len(v['tickets']) for v in issue_cats.values() if v['disposition'] == 'REVIEW')

print("=" * 72)
print(f"  REPEATED TICKETS — ACTUAL ISSUE TYPE BREAKDOWN  (Total: {total})")
print("=" * 72)
print(f"  {'Issue Type':<36} {'Disposition':<12} {'Count':>5}  {'%':>5}")
print(f"  {'-' * 62}")
for key, info in issue_cats.items():
    n = len(info['tickets'])
    if n == 0:
        continue
    p = n / total * 100
    print(f"  {info['label']:<36} {info['disposition']:<12} {n:>5}  {p:>4.1f}%")
print(f"  {'-' * 62}")
print(f"  {'TOTAL':<36} {'':12} {total:>5}  100.0%")
print(f"\n  Safe to AUTO-CLOSE : {auto} tickets ({auto/total*100:.1f}%)")
print(f"  Needs REVIEW       : {review} tickets ({review/total*100:.1f}%)")
print(f"  MANUAL required    : {manual} tickets ({manual/total*100:.1f}%)")
print("=" * 72)

# --- PRINT IDs PER ACTUAL ISSUE TYPE ---
SEP = '-' * 72
for key, info in issue_cats.items():
    tix = info['tickets']
    if not tix:
        continue
    print(f"\n{SEP}")
    print(f"  {info['label'].upper()}  [{info['disposition']}]  --  {len(tix)} tickets")
    print(f"  (These are REPEATED but the underlying issue is: {info['label']})")
    print(SEP)
    ids = [str(t[0]) for t in tix]
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
