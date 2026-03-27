# -*- coding: utf-8 -*-
# Thorough recheck of 233 dropped-off tickets
# Check ALL comments (public + internal, JSON + plain text) for any real content
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

ids_233 = [
    12009854,12009814,12009794,12009777,12009695,12009658,12009599,12009560,
    12009203,12009121,12009031,12008960,12008953,12008942,12008848,12008823,
    12008689,12008663,12008642,12008075,12007625,12007519,12007495,12007049,
    12006708,12006680,12006509,12006250,12006025,12005844,12005730,12005727,
    12004387,12004228,12004089,12004012,12003862,12003728,12003713,12003642,
    12003584,12003566,12003561,12003478,12003456,12003452,12003432,12003396,
    12003272,12003126,12002981,12002833,12002650,12002537,12002495,12002471,
    12002414,12002268,12002215,12002192,12001999,12001703,12001594,12001478,
    12001473,12001223,12001127,12001054,12000551,12000213,12000191,12000170,
    12000104,11999705,11999639,11999496,11999386,11999247,11999236,11999227,
    11999080,11998739,11998737,11998655,11998549,11998393,11998386,11998132,
    11997856,11997831,11997811,11997754,11997596,11997283,11996991,11996906,
    11996844,11996767,11996677,11996571,11996481,11996464,11996158,11995643,
    11995601,11995419,11995226,11995096,11995032,11995023,11994998,11994956,
    11994864,11994669,11994488,11994097,11993848,11992996,11992765,11992646,
    11992631,11992496,11992485,11992414,11992348,11992124,11991983,11991729,
    11991714,11991605,11991183,11990392,11990342,11990115,11989829,11989402,
    11989121,11989091,11988919,11988694,11988157,11987968,11987500,11987370,
    11987335,11987165,11986991,11986873,11986872,11986321,11986103,11985450,
    11984020,11983757,11982507,11982066,11981754,11981406,11981181,11980711,
    11980289,11980099,11980073,11979752,11979695,11979200,11979181,11979104,
    11979085,11978971,11978930,11978404,11977917,11977536,11977520,11976805,
    11976561,11976544,11976507,11976318,11976232,11976084,11975485,11975307,
    11975212,11974910,11974166,11972493,11971831,11970989,11968326,11967502,
    11965422,11965326,11964105,11962984,11962458,11961372,11958693,11955113,
    11950748,11950447,11946716,11941330,11939546,11937563,11933757,11925568,
    11921172,11918461,11906379,11905009,11903164,11902838,11902830,11901217,
    11899097,11888459,11887369,11884353,11883181,11883146,11880594,11878131,
    11875613,11843182,11802594,11801206,11785874,11775843,11769286,11756278,
    11733214
]

placeholders = ','.join(['%s'] * len(ids_233))

# Fetch ALL comments (public + internal) for each ticket
cur.execute(f"""
    SELECT t.id, t.source, t.created,
           tc.comment, tc.is_internal
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.id IN ({placeholders})
    ORDER BY t.id, tc.created
""", ids_233)
rows = cur.fetchall()

# Agent boilerplate phrases — if comment only contains these, it's not real content
AGENT_PHRASES = [
    'good morning', 'good afternoon', 'good evening', 'we apologize',
    'inconvenience', 'highlighted the issue', 'relevant team', 'allow us',
    'sorry to hear', 'sincerely apologize', 'i have escalated',
    'we will check', 'we will look into', 'we take this seriously',
    'please allow', 'kindly allow', 'we will get back',
    'thank you for', 'thanks for reaching', 'we have noted',
    'rest assured', 'we will resolve', 'i hope this helps',
]

SAGE_NOISE = [
    'sage', 'cityflo support assistant', 'please choose an option',
    'hey there', 'what can i assist', 'main menu', 'choose from',
    'how can i help', 'select an option',
]

def is_real_content(text, source):
    if not text or len(text.strip()) < 5:
        return False
    t = text.lower().strip()

    # Skip Sage menu noise
    if any(n in t for n in SAGE_NOISE):
        return False

    # Skip pure agent boilerplate
    if any(p in t for p in AGENT_PHRASES):
        return False

    # For Sage JSON, extract yellow bubble text
    try:
        data = json.loads(text)
        yellow_texts = []
        def walk(obj, in_yellow=False):
            if isinstance(obj, dict):
                yellow = obj.get('background', '') == '#FFEEC0' or in_yellow
                if obj.get('type') == 'Text' and obj.get('value') and yellow:
                    yellow_texts.append(obj['value'].strip())
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        walk(v, yellow)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, in_yellow)
        walk(data)
        if yellow_texts:
            combined = ' '.join(yellow_texts).lower()
            if any(n in combined for n in SAGE_NOISE):
                return False
            if len(combined.strip()) > 5:
                return True
        return False
    except Exception:
        # Plain text — if it's not agent boilerplate, it's real
        return len(t) > 8

# Group comments by ticket
from collections import defaultdict
ticket_comments = defaultdict(list)
ticket_meta = {}
for r in rows:
    tid, source, created, comment, is_internal = r
    ticket_meta[tid] = (source, created)
    if comment:
        ticket_comments[tid].append((comment, is_internal))

safe_auto_close = []
has_real_content = []

for tid in ids_233:
    source, created = ticket_meta.get(tid, ('?', None))
    comments = ticket_comments.get(tid, [])

    real_content = ''
    for comment, is_internal in comments:
        if is_real_content(comment, source):
            real_content = comment[:120].replace('\n', ' ')
            break

    if real_content:
        has_real_content.append((tid, source, created, real_content))
    else:
        safe_auto_close.append((tid, source, created))

print("=" * 78)
print(f"  RECHECK RESULT — 233 DROPPED-OFF TICKETS")
print("=" * 78)
print(f"\n  Safe to AUTO-CLOSE (truly empty) : {len(safe_auto_close)}")
print(f"  Has real content — EXCLUDE       : {len(has_real_content)}")

print()
print("=" * 78)
print(f"  EXCLUDE — HAS REAL CONTENT ({len(has_real_content)} tickets)")
print("=" * 78)
print(f"\n  {'Ticket ID':<13} {'Src':<5} {'Date':<13} Content found")
print(f"  {'-'*74}")
for tid, source, created, content in has_real_content:
    src = {'1':'App','7':'Social','8':'IO/WA','9':'Sage'}.get(source, source)
    print(f"  #{tid:<12} {src:<5} {created.strftime('%d %b %Y'):<13} {content[:55]}")

print()
print("=" * 78)
print(f"  FINAL CLEAN AUTO-CLOSE LIST — {len(safe_auto_close)} tickets")
print("=" * 78)
ids = sorted([r[0] for r in safe_auto_close], reverse=True)
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

cur.close()
conn.close()
