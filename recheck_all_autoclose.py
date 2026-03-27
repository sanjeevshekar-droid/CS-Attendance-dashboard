# -*- coding: utf-8 -*-
# Thorough recheck of ALL auto-close category tickets (AC, Seat, Hygiene, Tracking, Suggestion)
# Checks ALL comments (public + internal, JSON + plain text) for any real IO/plain-text content
import sys, io, psycopg2, json
from collections import defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

GHOST_TICKETS = {
    12009712,12009689,12009605,12009593,12009553,12009412,12009406,12009376,
    12009365,12009331,12009325,12009228,12009123,12008877,12008805,12008737,
    12008711,12008660,12008510,12007485,12007374,12007330,12007276,12007213,
    12007155,12007052,12007037,12007021,12006907,12006804,12006744,12005256,
    12005076,12005009,12004993,12004953,12004950,12004741,12004686,12004605,
    12004492,12004261,12004195,12004182,12004154,12004151,12003220,12003014,
    12002908,12002869,12002272,12002184,12001475,11999270
}

# Fetch all open tickets with ALL their comments
cur.execute("""
    SELECT t.id, t.source, t.info_tag, t.created,
           t.sageai_category_slug,
           tc.comment, tc.is_internal
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1'
      AND t.created >= NOW() - INTERVAL '30 days'
    ORDER BY t.id, tc.created
""")
rows = cur.fetchall()

# Group all comments per ticket
ticket_comments = defaultdict(list)
ticket_meta = {}
for r in rows:
    tid, source, info_tag, created, sage_cat, comment, is_internal = r
    ticket_meta[tid] = (source, info_tag, created, sage_cat)
    if comment:
        ticket_comments[tid].append((comment, bool(is_internal)))

AGENT_PHRASES = [
    'good morning', 'good afternoon', 'good evening', 'we apologize',
    'inconvenience', 'highlighted the issue', 'relevant team', 'allow us',
    'sorry to hear', 'sincerely apologize', 'i have escalated',
    'we will check', 'we will look into', 'we take this seriously',
    'please allow', 'kindly allow', 'we will get back',
    'thank you for', 'thanks for reaching', 'we have noted',
    'rest assured', 'we will resolve', 'i hope this helps',
    'we kindly request you to please elaborate',
    'kindly elaborate', 'please elaborate',
]

SAGE_NOISE = [
    'cityflo support assistant', 'please choose an option', 'hey there',
    'what can i assist', 'main menu', 'choose from below',
    'how can i help', 'select an option',
]

def get_yellow_text(text):
    try:
        data = json.loads(text)
        msgs = []
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    msgs.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, in_yellow)
        walk(data)
        return ' '.join(msgs)
    except Exception:
        return ''

def has_real_content(comments, source):
    """Return (bool, snippet) — True if ticket has real non-Sage content."""
    for comment, is_internal in comments:
        t = comment.strip().lower()
        if len(t) < 5:
            continue
        if any(p in t for p in AGENT_PHRASES):
            continue

        # Try JSON (Sage)
        yellow = get_yellow_text(comment)
        if yellow:
            y = yellow.lower()
            if any(n in y for n in SAGE_NOISE):
                continue
            if len(y.strip()) > 5:
                return True, yellow[:100]
        else:
            # Plain text (non-Sage: App feedback, IO/WhatsApp, Social)
            if any(n in t for n in SAGE_NOISE):
                continue
            if len(t) > 8:
                return True, comment.strip()[:100]
    return False, ''

def get_customer_text_only(comments):
    """Yellow bubble text only — for classification."""
    msgs = []
    for comment, is_internal in comments:
        if is_internal:
            continue
        yellow = get_yellow_text(comment)
        if yellow:
            msgs.append(yellow)
        else:
            try:
                json.loads(comment)
            except Exception:
                pass  # skip plain-text public (agent reply)
    return ' '.join(msgs).lower()

def label(cust):
    if any(k in cust for k in ['i lost or found something','i lost something','i found something',
            'earphone','earpod','earbuds','airpod','left my bottle','forgot my bottle',
            'left my bag','forgot my bag','lost my bag','left my phone','forgot my phone',
            'lost my phone','lost my wallet','forgot my wallet','left my charger',
            'left my keys','forgot my keys','left my glasses','left my umbrella',
            'rsa token','lost one item','pouch on seat','tiffin','lunch box','lunchbox',
            'forgot on seat','left on bus','left in bus','left behind on seat','forgot on bus']):
        return 'LOST_FOUND', 'Lost and Found', 'MANUAL'
    if any(k in cust for k in ['i have an issue with driver','i had an issue with driver',
            'driving rashly','rash driving','behaving rudely','rude','wrong route',
            'took other route','unscheduled stop','talking on phone','talking on call',
            'honking','driving very slowly','driving slow','other driver issue',
            "didn't stop at designated",'misbehaved','unprofessional driver']):
        return 'DRIVER','Driver Behaviour Issue','MANUAL'
    if any(k in cust for k in ['i have an issue with ac','i had an issue with ac',
            'ac is not working','ac was not working','ac not working',
            'increase the ac temperature','decrease the ac temperature',
            'ac was very cold','ac was very hot','ac vent is broken','ac vent was broken',
            'ac is making noise','ac was making noise','no ac','ac off','no cooling','ac problem']):
        return 'AC','AC Issue','AUTO-CLOSE'
    if any(k in cust for k in ['my seat has a problem','my seat had a problem',
            'slider is not working','slider was not working','handrest is broken',
            'handrest was broken','recliner is not working','recliner was not working',
            'footrest is broken','footrest was broken','charging point is not working',
            'charging point was not working','bottle holder is broken','seat pocket is broken']):
        return 'SEAT','Seat / Hardware Issue','AUTO-CLOSE'
    if any(k in cust for k in ['bus quality and hygiene issue','bus was not clean',
            'bus is not clean','water is dripping in the bus','water was dripping',
            'bus making noise','bus was making noise','flies in the bus',
            'bus broke down','bus is in poor condition','bus was in poor condition']):
        return 'HYGIENE','Bus Quality / Hygiene','AUTO-CLOSE'
    if any(k in cust for k in ['where is my bus',"i can't track the bus",
            "i couldn't track the bus",'tracking is wrong','tracking was wrong',
            'the bus is not moving','the bus was not moving']):
        return 'TRACKING','Tracking Issue','AUTO-CLOSE'
    if any(k in cust for k in ['i want to reschedule','i missed my bus','i want a later bus',
            'i want an earlier bus','i want to change pickup stop','i want to cancel this ride']):
        return 'RESCHEDULE','Reschedule / Cancellation','REVIEW'
    if any(k in cust for k in ['the bus was late','the bus is late','the bus left early',
            'the bus did not wait',"the bus didn't wait",'bus left before time']):
        return 'BUS_TIMING','Bus Timing (Late / Early)','REVIEW'
    if any(k in cust for k in ['i have a payment related issue','i had a payment related issue',
            'i want refund','amount deducted','amount not refunded','double charge','charged twice',
            'i want payment invoice','paid multiple times by mistake']):
        return 'PAYMENT','Payment / Refund / Invoice','REVIEW'
    if any(k in cust for k in ['app issue','app not working','unable to book',
            'unable to cancel','unable to reschedule','cannot book','login issue']):
        return 'APP','App / Booking Issue','REVIEW'
    if any(k in cust for k in ['suggestions: route, timing, stop','existing route',
            'subscription','referral','suggestions','b2b','rentals','other']):
        return 'SUGGESTION','Route / Timing Suggestion','AUTO-CLOSE'
    if len(cust.strip()) < 10:
        return 'MENU_ONLY','No Issue Stated','AUTO-CLOSE'
    return 'UNKNOWN','Other / Unclear','REVIEW'

# Classify all tickets
AUTO_CLOSE_CATS = {'AC','SEAT','HYGIENE','TRACKING','SUGGESTION'}

results = defaultdict(lambda: {'clean': [], 'has_content': []})

for tid, (source, info_tag, created, sage_cat) in ticket_meta.items():
    if tid in GHOST_TICKETS:
        continue
    comments = ticket_comments.get(tid, [])
    cust = get_customer_text_only(comments)
    cat_key, cat_label, disposition = label(cust)

    if disposition != 'AUTO-CLOSE' or cat_key == 'MENU_ONLY':
        continue  # skip non-auto-close and dropped-off (already handled)

    real, snippet = has_real_content(comments, source)
    src_label = {'1':'App','7':'Social','8':'IO/WA','9':'Sage'}.get(source, source)

    entry = {
        'id': tid,
        'source': source,
        'src_label': src_label,
        'date': created.strftime('%d %b %Y'),
        'repeated': info_tag == 'Repeated',
        'snippet': snippet,
        'cat_label': cat_label,
    }

    if real and source in ('8','7'):
        # IO/Social with actual content — should not auto-close
        results[cat_key]['has_content'].append(entry)
    elif real and source == '1':
        # App plain-text feedback — could be post-ride rating comment
        # These are ride feedback, not open issues needing CS — still safe to auto-close
        # UNLESS it's a real complaint
        complaint_kw = ['broken','not working','driver','refund','late','missing',
                        'forgot','lost','rash','rude','not stop','didn\'t stop',
                        'didn\'t halt','not halt','early','noise','dirty','mosquito',
                        'cancel','reschedule','invoice','deduct','wallet','hygiene',
                        'tracking','fell','fell off','damaged','unsafe']
        is_complaint = any(k in snippet.lower() for k in complaint_kw)
        if is_complaint:
            results[cat_key]['has_content'].append(entry)
        else:
            results[cat_key]['clean'].append(entry)
    else:
        results[cat_key]['clean'].append(entry)

CAT_META = [
    ('AC',         'AC Issue',                 'AUTO-CLOSE'),
    ('SEAT',       'Seat / Hardware Issue',    'AUTO-CLOSE'),
    ('HYGIENE',    'Bus Quality / Hygiene',    'AUTO-CLOSE'),
    ('TRACKING',   'Tracking Issue',           'AUTO-CLOSE'),
    ('SUGGESTION', 'Route/Timing Suggestion',  'AUTO-CLOSE'),
]

print("=" * 78)
print("  THOROUGH RECHECK — ALL AUTO-CLOSE CATEGORIES")
print("  Checking ALL comments (public + internal, JSON + plain text)")
print("=" * 78)
print(f"\n  {'Category':<30} {'Total':>6}  {'Clean Auto-Close':>16}  {'Needs Review':>12}")
print(f"  {'-'*68}")

for cat_key, cat_label, _ in CAT_META:
    clean = results[cat_key]['clean']
    flagged = results[cat_key]['has_content']
    total = len(clean) + len(flagged)
    print(f"  {cat_label:<30} {total:>6}  {len(clean):>16}  {len(flagged):>12}")

all_clean = sum(len(results[k]['clean']) for k,_,_ in CAT_META)
all_flagged = sum(len(results[k]['has_content']) for k,_,_ in CAT_META)
print(f"  {'-'*68}")
print(f"  {'TOTAL':<30} {all_clean+all_flagged:>6}  {all_clean:>16}  {all_flagged:>12}")

# --- Per category detail ---
for cat_key, cat_label, _ in CAT_META:
    clean = results[cat_key]['clean']
    flagged = results[cat_key]['has_content']

    print()
    print("=" * 78)
    print(f"  {cat_label.upper()}")
    print("=" * 78)

    if flagged:
        print(f"\n  EXCLUDE — IO/complaint content found ({len(flagged)} tickets):")
        print(f"  {'Ticket ID':<13} {'Src':<7} {'Date':<13} Content")
        print(f"  {'-'*70}")
        for e in flagged:
            print(f"  #{e['id']:<12} {e['src_label']:<7} {e['date']:<13} {e['snippet'][:45]}")
        excl_ids = [str(e['id']) for e in flagged]
        print(f"\n  Exclude IDs:")
        for i in range(0, len(excl_ids), 8):
            print('  ' + '   '.join(f'#{x}' for x in excl_ids[i:i+8]))

    print(f"\n  CLEAN AUTO-CLOSE ({len(clean)} tickets):")
    ids = sorted([e['id'] for e in clean], reverse=True)
    for i in range(0, len(ids), 8):
        print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
