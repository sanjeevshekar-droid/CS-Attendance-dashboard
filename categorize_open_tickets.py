import psycopg2, json
from collections import defaultdict

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

# Step 1: Fetch all open tickets (last 30 days) with their comments
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
print(f"Total open tickets fetched: {len(all_tickets)}\n")

def extract_all_text(comment_str):
    """Extract all visible text from a comment (JSON or plain)."""
    texts = []
    try:
        data = json.loads(comment_str)
        def walk(obj):
            if isinstance(obj, dict):
                if obj.get('type') == 'Text' and obj.get('value'):
                    v = obj['value'].strip()
                    if v:
                        texts.append(v)
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)
        walk(data)
    except Exception:
        if comment_str and len(comment_str) > 5:
            texts.append(comment_str[:300])
    return texts

def get_full_text(all_comments_raw):
    if not all_comments_raw:
        return ""
    full = []
    for c in all_comments_raw.split('|||'):
        full.extend(extract_all_text(c))
    return ' '.join(full).lower()

# Step 2: Categorize each ticket
categories = {
    'CAT1_REPEATED':    [],
    'CAT2_SUGGESTION':  [],
    'CAT3_AC':          [],
    'CAT4_SEAT':        [],
    'CAT5_HYGIENE':     [],
    'CAT6_TRACKING':    [],
    'CAT7_LOST_FOUND':  [],
    'CAT8_DRIVER':      [],
    'CAT9_BUS_TIMING':  [],
    'CAT10_PAYMENT':    [],
    'CAT11_APP':        [],
    'CAT12_MANUAL_ONLY':[],   # source 8/7 with no Sage flow
    'UNCATEGORIZED':    [],
}

for row in all_tickets:
    tid, source, info_tag, created, all_comments = row
    text = get_full_text(all_comments)
    date_str = created.strftime('%d %b %Y')

    # --- CAT 1: REPEATED (info_tag = Repeated, regardless of issue type) ---
    if info_tag == 'Repeated':
        categories['CAT1_REPEATED'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 7: LOST & FOUND (check first before generic keywords) ---
    if any(k in text for k in ['lost something', 'found something', 'i lost', 'i found', 'forgot my', 'left my',
                                 'earphone', 'earpod', 'earbuds', 'wallet', 'bag', 'phone', 'rsa token',
                                 'left behind', 'left it on', 'seat 5b', 'seat 7d']):
        categories['CAT7_LOST_FOUND'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 8: DRIVER ISSUES ---
    if any(k in text for k in ['driving rashly', 'rash driving', 'rude', 'behaving', 'unprofessional',
                                 'driver was', 'driver is', 'wrong route', 'unscheduled stop',
                                 'talking on phone', 'talking on call', 'honking', 'driving slow',
                                 'other driver', 'driver issue', 'i have an issue with driver',
                                 'i had an issue with driver']):
        categories['CAT8_DRIVER'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 3: AC ISSUES ---
    if any(k in text for k in ['ac is not working', 'ac was not working', 'ac not working',
                                 'i have an issue with ac', 'i had an issue with ac',
                                 'increase the ac', 'decrease the ac', 'ac temperature',
                                 'too cold', 'ac was very cold', 'too hot', 'ac was very hot',
                                 'ac vent', 'ac is making noise', 'informed the driver to decrease']):
        categories['CAT3_AC'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 4: SEAT / HARDWARE ---
    if any(k in text for k in ['my seat has a problem', 'my seat had a problem',
                                 'slider is not working', 'slider was not working',
                                 'handrest', 'recliner', 'footrest', 'charging point',
                                 'bottle holder', 'seat pocket', 'seat related issue',
                                 'seat-related']):
        categories['CAT4_SEAT'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 5: HYGIENE / BUS QUALITY ---
    if any(k in text for k in ['bus quality and hygiene', 'not clean', 'bus was not clean',
                                 'hygiene', 'dirty', 'smell', 'flies in the bus', 'flies were',
                                 'water is dripping', 'water was dripping', 'water dripping',
                                 'bus broke down', 'bus breakdown', 'bus making noise',
                                 'bus was in poor condition', 'poor condition', 'bus quality']):
        categories['CAT5_HYGIENE'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 6: TRACKING ---
    if any(k in text for k in ['where is my bus', 'tracking issue', 'tracking was wrong',
                                 'tracking is wrong', 'cannot track', "can't track",
                                 "couldn't track", 'bus is not moving', 'bus was not moving',
                                 'unable to track', 'i was unable to track']):
        categories['CAT6_TRACKING'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 9: BUS TIMING ---
    if any(k in text for k in ['bus is late', 'bus was late', 'the bus was late',
                                 'bus left early', 'bus departed early', 'bus did not wait',
                                 'did not pick', 'missed my bus', 'i missed my bus',
                                 'the bus was late', 'late', 'early']):
        if 'late' in text or 'early' in text or 'missed' in text:
            categories['CAT9_BUS_TIMING'].append((tid, date_str, source, text[:80]))
            continue

    # --- CAT 2: ROUTE / SUGGESTIONS ---
    if any(k in text for k in ['suggestions: route', 'suggestion', 'existing route',
                                 'new route', 'new stop', 'route suggestion', 'timing suggestion',
                                 'stop suggestion', 'shuttles for', 'add a route', 'add route',
                                 'start a route', 'via dwarka', 'b2b', 'rentals']):
        categories['CAT2_SUGGESTION'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 10: PAYMENT ---
    if any(k in text for k in ['payment', 'refund', 'wallet', 'invoice', 'amount deducted',
                                 'deducted', 'paid multiple', 'bank account', 'money',
                                 'i have a payment', 'i had a payment', 'billing']):
        categories['CAT10_PAYMENT'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 11: APP / BOOKING ---
    if any(k in text for k in ['app issue', 'app not working', 'slow app', 'unable to book',
                                 'unable to cancel', 'unable to reschedule', 'boarding pass',
                                 'app is not', 'app was not', 'booking issue', 'cannot book',
                                 "can't book", 'update the app', 'reinstall']):
        categories['CAT11_APP'].append((tid, date_str, source, text[:80]))
        continue

    # --- CAT 12: MANUAL CHANNELS (source 8 / 7) ---
    if source in ('8', '7'):
        categories['CAT12_MANUAL_ONLY'].append((tid, date_str, source, text[:80]))
        continue

    # --- UNCATEGORIZED ---
    categories['UNCATEGORIZED'].append((tid, date_str, source, text[:80]))


# Step 3: Print results
labels = {
    'CAT1_REPEATED':    ('Repeated Tickets',                  'AUTO-CLOSE',   'Deduplicate — link to existing open ticket'),
    'CAT2_SUGGESTION':  ('Route / Timing Suggestions',        'AUTO-CLOSE',   'Log & acknowledge — no CS action needed'),
    'CAT3_AC':          ('AC Issues',                         'AUTO-CLOSE',   'Driver already notified — close after 24h if no re-escalation'),
    'CAT4_SEAT':        ('Seat / Hardware Issues',            'AUTO-CLOSE',   'Acknowledgement sent, forwarded to maintenance'),
    'CAT5_HYGIENE':     ('Hygiene / Bus Condition',           'AUTO-CLOSE',   'Ops notified — no real-time fix possible'),
    'CAT6_TRACKING':    ('Tracking Complaints',               'AUTO-CLOSE',   'Check if ride ended — auto-close if yes'),
    'CAT7_LOST_FOUND':  ('Lost & Found',                      'MANUAL',       'Needs driver/ops coordination'),
    'CAT8_DRIVER':      ('Driver Behaviour Issues',           'MANUAL',       'Needs investigation'),
    'CAT9_BUS_TIMING':  ('Bus Timing (Late / Early / Missed)','REVIEW',       'Notify ops — can auto-close for past rides'),
    'CAT10_PAYMENT':    ('Payment / Refund / Invoice',        'REVIEW',       'Invoice & duplicate charge can auto-close; refund disputes manual'),
    'CAT11_APP':        ('App / Booking Issues',              'REVIEW',       'Provide troubleshoot steps — auto-close if no reply in 24h'),
    'CAT12_MANUAL_ONLY':('Manual Channel (WhatsApp/Other)',   'MANUAL',       'Source 8/7 — bypass Sage, need agent'),
    'UNCATEGORIZED':    ('Uncategorized / Unknown',           'REVIEW',       'Needs triage'),
}

total = sum(len(v) for v in categories.values())
print(f"{'='*90}")
print(f"  OPEN TICKET CATEGORIZATION SUMMARY  (Total open tickets: {total})")
print(f"{'='*90}")
print(f"  {'Category':<35} {'Action':12} {'Count':6}  {'% of Total'}")
print(f"  {'-'*80}")
for key, (label, action, note) in labels.items():
    count = len(categories[key])
    pct = count / total * 100 if total else 0
    print(f"  {label:<35} {action:<12} {count:5}   {pct:5.1f}%")
print(f"  {'-'*80}")
print(f"  {'TOTAL':<35} {'':12} {total:5}   100.0%")

auto_count = sum(len(categories[k]) for k in ['CAT1_REPEATED','CAT2_SUGGESTION','CAT3_AC','CAT4_SEAT','CAT5_HYGIENE','CAT6_TRACKING'])
print(f"\n  Directly auto-closeable now:  {auto_count} tickets ({auto_count/total*100:.1f}%)")
review_count = sum(len(categories[k]) for k in ['CAT9_BUS_TIMING','CAT10_PAYMENT','CAT11_APP'])
print(f"  Partially automatable (review): {review_count} tickets ({review_count/total*100:.1f}%)")
manual_count = sum(len(categories[k]) for k in ['CAT7_LOST_FOUND','CAT8_DRIVER','CAT12_MANUAL_ONLY'])
print(f"  Needs manual resolution:      {manual_count} tickets ({manual_count/total*100:.1f}%)")
print()

# Step 4: Print ticket IDs per category
for key, (label, action, note) in labels.items():
    tickets = categories[key]
    if not tickets:
        continue
    print(f"\n{'='*90}")
    print(f"  {label.upper()}  [{action}]  —  {len(tickets)} tickets")
    print(f"  Note: {note}")
    print(f"{'='*90}")
    print(f"  {'Ticket ID':<12} {'Date':<14} {'Src':<5}  Conversation Snippet")
    print(f"  {'-'*80}")
    for tid, date_str, src, snippet in tickets:
        clean = snippet.replace('\n', ' ').replace('\r', '')[:65]
        print(f"  #{tid:<11} {date_str:<14} {str(src):<5}  {clean}")

cur.close()
conn.close()
