# -*- coding: utf-8 -*-
import sys, io, psycopg2, json
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT t.id, t.source, t.info_tag, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc
           ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
      AND t.created >= NOW() - INTERVAL '30 days'
    GROUP BY t.id, t.source, t.info_tag, t.created
    ORDER BY t.id DESC
""")
all_tickets = cur.fetchall()

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

cats = defaultdict(list)

AC_KW      = ['ac is not working','ac was not working','i have an issue with ac',
               'i had an issue with ac','increase the ac','decrease the ac',
               'ac temperature','ac was very cold','ac was very hot','ac vent',
               'ac is making noise','informed the driver to decrease']
SEAT_KW    = ['my seat has a problem','my seat had a problem','slider is not working',
               'slider was not working','handrest','recliner','footrest',
               'charging point','bottle holder','seat pocket']
HYGIENE_KW = ['bus quality and hygiene','bus is not clean','bus was not clean',
               'hygiene','dirty','flies in the bus','water is dripping',
               'water was dripping','bus broke down','bus making noise',
               'poor condition','smell','odour']
TRACK_KW   = ['where is my bus','tracking issue','tracking was wrong',
               'cannot track',"bus is not moving",'bus was not moving',
               'unable to track','i was unable to track']
LOST_KW    = ['lost something','found something','i lost','i found','forgot my',
               'left my','earphone','earpod','earbuds','rsa token','left behind',
               'umbrella','my bag','my phone']
DRIVER_KW  = ['driving rashly','rash driving','behaving rudely','rude',
               'wrong route','unscheduled stop','talking on phone','honking',
               'driving very slowly','i have an issue with driver',
               'i had an issue with driver','other driver issue']
TIMING_KW  = ['the bus was late','bus is late','bus was late','bus left early',
               'bus departed early','bus did not wait','did not pick me',
               'missed my bus','i missed my bus','bus skipped']
SUGGEST_KW = ['suggestions: route','existing route','new route','new stop',
               'shuttles for','add a route','start a route','b2b','rentals',
               'are there any buses','are there any shuttles','can you start']
PAYMENT_KW = ['payment','refund','wallet','invoice','amount deducted',
               'paid multiple','bank account','money deducted',
               'i have a payment','billing','not reflecting']
APP_KW     = ['app issue','app not working','slow app','unable to book',
               'unable to cancel','unable to reschedule','boarding pass',
               'cannot book','update the app','reinstall']

for row in all_tickets:
    tid, src, tag, created, raw = row
    t = get_text(raw)
    d = created.strftime('%d %b %Y')

    if tag == 'Repeated':
        cats['C1_REPEATED'].append((tid, d, src))
        continue
    if any(k in t for k in LOST_KW):
        cats['C7_LOST'].append((tid, d, src))
        continue
    if any(k in t for k in DRIVER_KW):
        cats['C8_DRIVER'].append((tid, d, src))
        continue
    if any(k in t for k in AC_KW):
        cats['C3_AC'].append((tid, d, src))
        continue
    if any(k in t for k in SEAT_KW):
        cats['C4_SEAT'].append((tid, d, src))
        continue
    if any(k in t for k in HYGIENE_KW):
        cats['C5_HYGIENE'].append((tid, d, src))
        continue
    if any(k in t for k in TRACK_KW):
        cats['C6_TRACKING'].append((tid, d, src))
        continue
    if any(k in t for k in TIMING_KW):
        cats['C9_TIMING'].append((tid, d, src))
        continue
    if any(k in t for k in SUGGEST_KW):
        cats['C2_SUGGEST'].append((tid, d, src))
        continue
    if any(k in t for k in PAYMENT_KW):
        cats['C10_PAYMENT'].append((tid, d, src))
        continue
    if any(k in t for k in APP_KW):
        cats['C11_APP'].append((tid, d, src))
        continue
    if src in ('8', '7'):
        cats['C12_MANUAL'].append((tid, d, src))
        continue
    cats['C13_UNKNOWN'].append((tid, d, src))

meta = [
    ('C1_REPEATED',  'Repeated Tickets',                 'AUTO-CLOSE'),
    ('C2_SUGGEST',   'Route / Timing Suggestions',       'AUTO-CLOSE'),
    ('C3_AC',        'AC Issues',                        'AUTO-CLOSE'),
    ('C4_SEAT',      'Seat / Hardware Issues',           'AUTO-CLOSE'),
    ('C5_HYGIENE',   'Hygiene / Bus Condition',          'AUTO-CLOSE'),
    ('C6_TRACKING',  'Tracking Complaints',              'AUTO-CLOSE'),
    ('C7_LOST',      'Lost and Found',                   'MANUAL    '),
    ('C8_DRIVER',    'Driver Behaviour Issues',          'MANUAL    '),
    ('C9_TIMING',    'Bus Timing (Late/Early/Missed)',   'REVIEW    '),
    ('C10_PAYMENT',  'Payment / Refund / Invoice',       'REVIEW    '),
    ('C11_APP',      'App / Booking Issues',             'REVIEW    '),
    ('C12_MANUAL',   'Manual Channel (WhatsApp/Other)',  'MANUAL    '),
    ('C13_UNKNOWN',  'Uncategorized / Unknown',          'REVIEW    '),
]

total = sum(len(v) for v in cats.values())

print()
print('=' * 68)
print(f'  OPEN TICKET BREAKDOWN  --  Total: {total} tickets (last 30 days)')
print('=' * 68)
print(f'  {"#":<3} {"Category":<34} {"Action":<12} {"Count":>5}  {"%":>5}')
print(f'  {"-" * 62}')
for i, (k, lbl, act) in enumerate(meta, 1):
    n = len(cats[k])
    p = n / total * 100
    print(f'  {i:<3} {lbl:<34} {act:<12} {n:>5}  {p:>4.1f}%')
print(f'  {"-" * 62}')
print(f'  {"TOTAL":<49} {total:>5}  100.0%')

ac = sum(len(cats[k]) for k in ['C1_REPEATED','C2_SUGGEST','C3_AC','C4_SEAT','C5_HYGIENE','C6_TRACKING'])
rv = sum(len(cats[k]) for k in ['C9_TIMING','C10_PAYMENT','C11_APP','C13_UNKNOWN'])
mn = sum(len(cats[k]) for k in ['C7_LOST','C8_DRIVER','C12_MANUAL'])

print(f'\n  AUTO-CLOSE now  : {ac:>4} tickets  ({ac/total*100:.1f}%)')
print(f'  REVIEW needed   : {rv:>4} tickets  ({rv/total*100:.1f}%)')
print(f'  MANUAL required : {mn:>4} tickets  ({mn/total*100:.1f}%)')
print('=' * 68)

SEP = '-' * 68
for k, lbl, act in meta:
    tix = cats[k]
    if not tix:
        continue
    print(f'\n{SEP}')
    print(f'  {lbl.upper()}  [{act.strip()}]  --  {len(tix)} tickets')
    print(SEP)
    ids = [str(r[0]) for r in tix]
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
