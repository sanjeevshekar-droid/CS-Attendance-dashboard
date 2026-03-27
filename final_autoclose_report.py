# -*- coding: utf-8 -*-
# Final open ticket recheck — excludes 54 blank App ghost tickets
# Classifies by customer message only (yellow bubbles)
# Reports AUTO-CLOSE ticket IDs per category
import sys, io, psycopg2, json
from collections import defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

# 54 blank App ghost tickets — handled separately, excluded from this report
GHOST_TICKETS = {
    12009712,12009689,12009605,12009593,12009553,12009412,12009406,12009376,
    12009365,12009331,12009325,12009228,12009123,12008877,12008805,12008737,
    12008711,12008660,12008510,12007485,12007374,12007330,12007276,12007213,
    12007155,12007052,12007037,12007021,12006907,12006804,12006744,12005256,
    12005076,12005009,12004993,12004953,12004950,12004741,12004686,12004605,
    12004492,12004261,12004195,12004182,12004154,12004151,12003220,12003014,
    12002908,12002869,12002272,12002184,12001475,11999270
}

cur.execute("""
    SELECT t.id, t.source, t.info_tag, t.created,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
    AND t.created >= NOW() - INTERVAL '30 days'
    GROUP BY t.id, t.source, t.info_tag, t.created
    ORDER BY t.created DESC
""")
all_rows = cur.fetchall()
rows = [r for r in all_rows if r[0] not in GHOST_TICKETS]
print(f"Total open tickets (excl. 54 ghost tickets): {len(rows)}\n")

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
    return ' '.join(msgs).lower()

def label_from_customer(cust):
    if any(k in cust for k in ['i lost or found something', 'i lost something', 'i found something',
                                'earphone','earpod','earbuds','airpod',
                                'left my bottle','forgot my bottle','water bottle on seat',
                                'left my bag','forgot my bag','lost my bag',
                                'left my phone','forgot my phone','lost my phone',
                                'lost my wallet','forgot my wallet',
                                'left my charger','forgot my charger',
                                'left my keys','forgot my keys','lost my keys',
                                'left my glasses','forgot my glasses',
                                'left my umbrella','forgot my umbrella',
                                'rsa token','lost one item','pouch on seat',
                                'tiffin','lunch box','lunchbox',
                                'forgot on seat','left on bus','left in bus',
                                'left behind on seat','forgot on bus']):
        return 'LOST_FOUND', 'Lost and Found', 'MANUAL'

    if any(k in cust for k in ['i have an issue with driver','i had an issue with driver',
                                'driving rashly','rash driving','behaving rudely','rude',
                                'wrong route','took other route','unscheduled stop',
                                'talking on phone','talking on call','honking',
                                'driving very slowly','driving slow',
                                'other driver issue',"didn't stop at designated",
                                'misbehaved','unprofessional driver']):
        return 'DRIVER', 'Driver Behaviour Issue', 'MANUAL'

    if any(k in cust for k in ['i have an issue with ac','i had an issue with ac',
                                'ac is not working','ac was not working','ac not working',
                                'increase the ac temperature','decrease the ac temperature',
                                'ac was very cold','ac was very hot',
                                'ac vent is broken','ac vent was broken',
                                'ac is making noise','ac was making noise',
                                'no ac','ac off','no cooling','ac problem']):
        return 'AC', 'AC Issue', 'AUTO-CLOSE'

    if any(k in cust for k in ['my seat has a problem','my seat had a problem',
                                'slider is not working','slider was not working',
                                'handrest is broken','handrest was broken',
                                'recliner is not working','recliner was not working',
                                'footrest is broken','footrest was broken',
                                'charging point is not working','charging point was not working',
                                'bottle holder is broken','bottle holder was broken',
                                'seat pocket is broken','seat pocket was broken']):
        return 'SEAT', 'Seat / Hardware Issue', 'AUTO-CLOSE'

    if any(k in cust for k in ['bus quality and hygiene issue','bus was not clean','bus is not clean',
                                'water is dripping in the bus','water was dripping',
                                'bus making noise','bus was making noise',
                                'flies in the bus','flies were in the bus',
                                'bus broke down','bus is in poor condition',
                                'bus was in poor condition']):
        return 'HYGIENE', 'Bus Quality / Hygiene', 'AUTO-CLOSE'

    if any(k in cust for k in ['where is my bus',"i can't track the bus","i couldn't track the bus",
                                'tracking is wrong','tracking was wrong',
                                'the bus is not moving','the bus was not moving']):
        return 'TRACKING', 'Tracking Issue', 'AUTO-CLOSE'

    if any(k in cust for k in ['i want to reschedule my ride','i wanted to reschedule',
                                'i want a later bus','i wanted a later bus',
                                'i want an earlier bus','i wanted an earlier bus',
                                'i want to change pickup stop','i want to change drop stop',
                                'i want to change seat','i want to cancel this ride',
                                'i wanted to cancel this ride',
                                'i missed my bus']):
        return 'RESCHEDULE', 'Reschedule / Cancellation', 'REVIEW'

    if any(k in cust for k in ['the bus was late','the bus is late',
                                'the bus left early','the bus did not wait at my stop',
                                "the bus didn't wait",'bus left before time']):
        return 'BUS_TIMING', 'Bus Timing (Late / Early)', 'REVIEW'

    if any(k in cust for k in ['i have a payment related issue','i had a payment related issue',
                                'i want refund in cityflo wallet','i wanted refund in cityflo wallet',
                                'i want wallet balance to my bank','i wanted wallet balance to my bank',
                                'i want payment invoice','i wanted payment invoice',
                                'paid multiple times by mistake',
                                'my amount was deducted but ride was not booked',
                                'amount deducted','amount not refunded',
                                'double charge','charged twice']):
        return 'PAYMENT', 'Payment / Refund / Invoice', 'REVIEW'

    if any(k in cust for k in ['app issue','app not working','slow app',
                                'unable to book ride','unable to cancel/reschedule',
                                'unable to cancel','unable to reschedule',
                                'unable to book','app was not working',
                                'cannot book','app is slow','login issue']):
        return 'APP', 'App / Booking Issue', 'REVIEW'

    if any(k in cust for k in ['suggestions: route, timing, stop',
                                'existing route','subscription','safety',
                                'referral','suggestions','b2b','rentals','other']):
        return 'SUGGESTION', 'Route / Timing Suggestion', 'AUTO-CLOSE'

    if len(cust.strip()) < 10:
        return 'MENU_ONLY', 'No Issue Stated (Dropped Off)', 'AUTO-CLOSE'

    return 'UNKNOWN', 'Other / Unclear', 'REVIEW'

results = defaultdict(list)

for row in rows:
    tid, source, info_tag, created, raw = row
    cust = get_customer_text(raw)
    cat_key, cat_label, disposition = label_from_customer(cust)

    snippet = ''
    for chunk in cust.replace('\n', ' ').split('.'):
        s = chunk.strip()
        ignore = ['sage', 'cityflo support assistant', 'please choose', 'hey there', 'what can i']
        if len(s) > 8 and not any(x in s for x in ignore):
            snippet = s[:70]
            break
    if not snippet and len(cust.strip()) > 3:
        snippet = cust.strip()[:70]

    results[cat_key].append({
        'id': tid,
        'date': created.strftime('%d %b %Y'),
        'label': cat_label,
        'disposition': disposition,
        'snippet': snippet,
        'repeated': (info_tag == 'Repeated'),
    })

meta_order = [
    ('LOST_FOUND', 'Lost and Found',                 'MANUAL'),
    ('DRIVER',     'Driver Behaviour Issue',          'MANUAL'),
    ('PAYMENT',    'Payment / Refund / Invoice',      'REVIEW'),
    ('RESCHEDULE', 'Reschedule / Cancellation',       'REVIEW'),
    ('BUS_TIMING', 'Bus Timing (Late / Early)',       'REVIEW'),
    ('APP',        'App / Booking Issue',             'REVIEW'),
    ('UNKNOWN',    'Other / Unclear',                 'REVIEW'),
    ('AC',         'AC Issue',                        'AUTO-CLOSE'),
    ('SEAT',       'Seat / Hardware Issue',           'AUTO-CLOSE'),
    ('HYGIENE',    'Bus Quality / Hygiene',           'AUTO-CLOSE'),
    ('TRACKING',   'Tracking Issue',                  'AUTO-CLOSE'),
    ('SUGGESTION', 'Route / Timing Suggestion',       'AUTO-CLOSE'),
    ('MENU_ONLY',  'No Issue Stated (Dropped Off)',   'AUTO-CLOSE'),
]

total  = sum(len(v) for v in results.values())
auto   = sum(len(results[k]) for k,_,d in meta_order if d == 'AUTO-CLOSE')
review = sum(len(results[k]) for k,_,d in meta_order if d == 'REVIEW')
manual = sum(len(results[k]) for k,_,d in meta_order if d == 'MANUAL')

# Add ghost ticket counts back for full picture
ghost_auto_close = 54  # all 54 ghost tickets are auto-close

print("=" * 72)
print(f"  OPEN TICKET BREAKDOWN  (excl. 54 ghost tickets)")
print(f"  Classified using CUSTOMER messages only")
print("=" * 72)
print(f"  {'Category':<36} {'Action':<12} {'Total':>5}  {'Repeated':>8}")
print(f"  {'-'*64}")
for k, lbl, disp in meta_order:
    tix = results[k]
    if not tix: continue
    rep = sum(1 for t in tix if t['repeated'])
    print(f"  {lbl:<36} {disp:<12} {len(tix):>5}  {rep:>8}")
print(f"  {'-'*64}")
print(f"  {'TOTAL (excl. ghost)':<36} {'':12} {total:>5}")
print(f"\n  AUTO-CLOSE (excl. ghost) : {auto} tickets ({auto/total*100:.1f}%)")
print(f"  REVIEW needed            : {review} tickets ({review/total*100:.1f}%)")
print(f"  MANUAL required          : {manual} tickets ({manual/total*100:.1f}%)")
print(f"\n  + Ghost tickets (auto)   : {ghost_auto_close} tickets")
print(f"  GRAND TOTAL AUTO-CLOSE   : {auto + ghost_auto_close} tickets")
print("=" * 72)

# --- AUTO-CLOSE TICKET IDs PER CATEGORY ---
print()
print("=" * 72)
print("  AUTO-CLOSE TICKET IDs — BY CATEGORY")
print("=" * 72)

auto_cats = [k for k, _, d in meta_order if d == 'AUTO-CLOSE']
for k, lbl, disp in meta_order:
    if disp != 'AUTO-CLOSE':
        continue
    tix = results[k]
    if not tix:
        continue
    print(f"\n{'=' * 72}")
    print(f"  {lbl.upper()}  --  {len(tix)} tickets")
    print(f"{'=' * 72}")
    print(f"  {'Customer Message':<44} {'Date':<13}  Ticket ID")
    print(f"  {'-'*70}")
    for t in tix:
        title = (t['snippet'][:42] + '..') if len(t['snippet']) > 42 else t['snippet']
        if not title:
            title = t['label']
        rep = '[R]' if t['repeated'] else '   '
        print(f"  {title:<44} {t['date']:<13} #{t['id']} {rep}")
    ids = [str(t['id']) for t in tix]
    print(f"\n  IDs only:")
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

# --- GHOST TICKETS reminder ---
print()
print("=" * 72)
print("  GHOST TICKETS (Blank App duplicates) -- 54 tickets -- AUTO-CLOSE")
print("=" * 72)
print("  29 Verified duplicates (paired with a Sage ticket):")
verified = [
    12009712,12009689,12009605,12009553,12009412,12009365,12009331,12009325,
    12009228,12008877,12008805,12008510,12007374,12007037,12006804,12006744,
    12005076,12005009,12004953,12004950,12004686,12004605,12004182,12004151,
    12003014,12002908,12002869,12002184,12001475
]
for i in range(0, len(verified), 8):
    print('  ' + '   '.join(f'#{x}' for x in verified[i:i+8]))

print()
print("  25 Blank abandoned tickets (no Sage pair):")
abandoned = [
    12009593,12009406,12009376,12009123,12008737,12008711,12008660,12007485,
    12007330,12007276,12007213,12007155,12007052,12007021,12006907,12005256,
    12004993,12004741,12004492,12004261,12004195,12004154,12003220,12002272,
    11999270
]
for i in range(0, len(abandoned), 8):
    print('  ' + '   '.join(f'#{x}' for x in abandoned[i:i+8]))

cur.close()
conn.close()
