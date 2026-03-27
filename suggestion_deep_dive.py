# -*- coding: utf-8 -*-
# Deep dive into the 226 Route/Timing Suggestion tickets
# Check sageai subcategories and actual customer text to split genuine vs misrouted
import sys, io, psycopg2, json
from collections import defaultdict, Counter
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

cur.execute("""
    SELECT t.id, t.info_tag, t.created,
           t.sageai_category_slug, t.sageai_subcategory_slug,
           string_agg(tc.comment, '|||' ORDER BY tc.created) as all_comments
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id AND tc.is_internal = false
    WHERE t.status = '1'
      AND t.created >= NOW() - INTERVAL '30 days'
      AND (
          -- Sage category is suggestion
          t.sageai_category_slug = 'suggestion-route-stop-timing'
          -- OR classified by keyword in customer text
          OR EXISTS (
              SELECT 1 FROM support_ticketcomment tc2
              WHERE tc2.ticket_id = t.id
                AND tc2.is_internal = false
                AND tc2.comment LIKE '%%suggestion%%'
          )
      )
    GROUP BY t.id, t.info_tag, t.created, t.sageai_category_slug, t.sageai_subcategory_slug
    ORDER BY t.id DESC
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
    return ' '.join(msgs).lower()

def get_agent_text(raw):
    msgs = []
    if not raw:
        return ''
    for c in raw.split('|||'):
        try:
            json.loads(c)
        except Exception:
            if c and len(c) > 5:
                msgs.append(c[:300])
    return ' '.join(msgs).lower()

# Keywords that indicate a REAL issue (customer picked wrong menu)
REAL_ISSUE_KW = [
    'refund', 'cancel', 'payment', 'deduct', 'wallet', 'reschedule',
    'not working', 'app issue', 'unable to book', 'recliner', 'ac issue',
    'driver', 'delay', 'lost', 'found', 'invoice', 'overcharge',
    'not allow', 'cannot reschedule', 'not able to reschedule',
    'charged', 'amount deducted', 'seat booked', 'ride cancelled',
    'bus was late', 'bus is late', 'bus left early', 'missed my bus',
    'tracking', 'where is my bus', 'not clean', 'hygiene',
    'misbehaved', 'rash driving', 'rude', 'wrong route',
]

buckets = {
    'existing_route':    [],
    'new_route':         [],
    'new_stop':          [],
    'timing':            [],
    'subscription':      [],
    'safety':            [],
    'referral':          [],
    'b2b_rentals':       [],
    'other_sub':         [],
    'no_sub':            [],
    'wrong_menu':        [],
    'dropped_off':       [],
}

for row in rows:
    tid, info_tag, created, sage_cat, sage_sub, raw = row
    cust = get_customer_text(raw)
    agent = get_agent_text(raw)

    # Check if real issue misrouted
    is_wrong_menu = any(k in cust for k in REAL_ISSUE_KW)

    # Check if dropped off (no real content)
    skip_words = {'am', 'pm', 'suggestions:', 'route,', 'timing,', 'stop',
                  'existing', 'other', 'subscription', 'referral', 'suggestions',
                  'safety', 'b2b', 'rentals', 'yes', 'no'}
    filtered = ' '.join(w for w in cust.split() if w not in skip_words)
    is_dropped_off = len(filtered.strip()) < 12

    snippet = ''
    for chunk in cust.split('.'):
        s = chunk.strip()
        ignore = ['sage', 'cityflo support', 'please choose', 'hey there', 'what can i']
        if len(s) > 10 and not any(x in s for x in ignore):
            snippet = s[:80]
            break
    if not snippet:
        snippet = cust[:80] if len(cust) > 5 else '(no text)'

    entry = {
        'id': tid,
        'date': created.strftime('%d %b %Y'),
        'sub': sage_sub or 'none',
        'snippet': snippet,
        'repeated': info_tag == 'Repeated',
    }

    if is_wrong_menu:
        entry['issue_hint'] = next((k for k in REAL_ISSUE_KW if k in cust), '')
        buckets['wrong_menu'].append(entry)
    elif is_dropped_off:
        buckets['dropped_off'].append(entry)
    else:
        sub = (sage_sub or '').lower()
        if 'existing' in sub:
            buckets['existing_route'].append(entry)
        elif 'new-route' in sub or 'new_route' in sub:
            buckets['new_route'].append(entry)
        elif 'stop' in sub:
            buckets['new_stop'].append(entry)
        elif 'timing' in sub:
            buckets['timing'].append(entry)
        elif 'subscription' in sub:
            buckets['subscription'].append(entry)
        elif 'safety' in sub:
            buckets['safety'].append(entry)
        elif 'referral' in sub:
            buckets['referral'].append(entry)
        elif 'b2b' in sub or 'rental' in sub:
            buckets['b2b_rentals'].append(entry)
        elif sub and sub != 'none':
            buckets['other_sub'].append(entry)
        else:
            buckets['no_sub'].append(entry)

total = sum(len(v) for v in buckets.values())

print("=" * 78)
print(f"  SUGGESTION TICKETS — DEEP DIVE  (Total: {total})")
print("=" * 78)
print(f"\n  {'Sub-Category / Type':<35} {'Action':<12} {'Count':>5}  {'Repeated':>8}")
print(f"  {'-'*64}")

sub_meta = [
    ('existing_route', 'Existing Route Feedback',         'AUTO-CLOSE'),
    ('new_route',      'New Route Request',               'AUTO-CLOSE'),
    ('new_stop',       'New Stop Request',                'AUTO-CLOSE'),
    ('timing',         'Timing Change Request',           'AUTO-CLOSE'),
    ('subscription',   'Subscription Query',              'REVIEW'),
    ('safety',         'Safety Feedback',                 'REVIEW'),
    ('referral',       'Referral',                        'AUTO-CLOSE'),
    ('b2b_rentals',    'B2B / Rentals',                   'REVIEW'),
    ('other_sub',      'Other Sub-category',              'REVIEW'),
    ('no_sub',         'No sub-category (free text)',     'AUTO-CLOSE'),
    ('dropped_off',    'Dropped Off (no content)',        'AUTO-CLOSE'),
    ('wrong_menu',     'WRONG MENU — Real Issue',         'REVIEW'),
]

auto_total = 0
review_total = 0

for key, label, action in sub_meta:
    tix = buckets[key]
    if not tix:
        continue
    rep = sum(1 for t in tix if t['repeated'])
    print(f"  {label:<35} {action:<12} {len(tix):>5}  {rep:>8}")
    if action == 'AUTO-CLOSE':
        auto_total += len(tix)
    else:
        review_total += len(tix)

print(f"  {'-'*64}")
print(f"  {'TOTAL':<35} {'':12} {total:>5}")
print(f"\n  Safe AUTO-CLOSE  : {auto_total}")
print(f"  Needs REVIEW     : {review_total}  (these were incorrectly auto-close before)")
print("=" * 78)

# --- Show subcategory slug breakdown raw ---
print("\n  RAW SAGE SUBCATEGORY SLUGS (all suggestion tickets):")
sub_counter = Counter((row[4] or 'none') for row in rows)
for sub, cnt in sub_counter.most_common():
    print(f"    {sub:<45} {cnt:>4}")

# --- Wrong menu detail ---
if buckets['wrong_menu']:
    print()
    print("=" * 78)
    print(f"  WRONG MENU — REAL ISSUES MISROUTED TO SUGGESTION  ({len(buckets['wrong_menu'])} tickets)")
    print(f"  These should NOT be auto-closed — need CS review")
    print("=" * 78)
    print(f"\n  {'Ticket ID':<13} {'Date':<13} {'Actual Issue Hint':<25} Customer Text")
    print(f"  {'-'*76}")
    for t in buckets['wrong_menu']:
        print(f"  #{t['id']:<12} {t['date']:<13} {t.get('issue_hint',''):<25} {t['snippet'][:45]}")
    print(f"\n  IDs only:")
    ids = [str(t['id']) for t in buckets['wrong_menu']]
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

# --- Subscription / Safety / B2B detail ---
for key, label, action in sub_meta:
    if action == 'REVIEW' and buckets[key]:
        tix = buckets[key]
        print()
        print(f"  {label.upper()} — {len(tix)} tickets — needs REVIEW")
        ids = [str(t['id']) for t in tix]
        for i in range(0, len(ids), 8):
            print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))
        print(f"  Sample messages:")
        for t in tix[:5]:
            print(f"    #{t['id']}: {t['snippet'][:80]}")

cur.close()
conn.close()
