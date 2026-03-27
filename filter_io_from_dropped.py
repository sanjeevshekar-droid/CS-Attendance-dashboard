# -*- coding: utf-8 -*-
# Filter IO (WhatsApp source=8) and social (source=7) tickets from dropped-off list
# Check internal comments to see if they have actual issues logged by associates
import sys, io, psycopg2, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

dropped_off_ids = [
    12009854,12009814,12009794,12009777,12009695,12009658,12009639,12009599,
    12009560,12009479,12009477,12009390,12009311,12009203,12009146,12009121,
    12009031,12008960,12008953,12008942,12008848,12008847,12008823,12008749,
    12008742,12008689,12008663,12008642,12008509,12008422,12008315,12008314,
    12008105,12008075,12007960,12007939,12007799,12007670,12007625,12007621,
    12007519,12007495,12007260,12007205,12007049,12006777,12006708,12006683,
    12006680,12006509,12006250,12006025,12005844,12005730,12005727,12004387,
    12004228,12004089,12004012,12003862,12003728,12003713,12003642,12003584,
    12003566,12003561,12003478,12003456,12003452,12003432,12003396,12003272,
    12003126,12002981,12002847,12002833,12002762,12002650,12002537,12002495,
    12002471,12002414,12002268,12002225,12002215,12002192,12001999,12001778,
    12001703,12001607,12001594,12001478,12001473,12001223,12001127,12001054,
    12001053,12000645,12000551,12000450,12000213,12000191,12000170,12000104,
    11999946,11999705,11999639,11999496,11999386,11999247,11999236,11999227,
    11999080,11998739,11998737,11998655,11998549,11998393,11998386,11998132,
    11997856,11997831,11997811,11997788,11997754,11997596,11997283,11996991,
    11996906,11996844,11996767,11996677,11996571,11996481,11996464,11996158,
    11995826,11995643,11995601,11995419,11995226,11995192,11995096,11995032,
    11995023,11994998,11994956,11994864,11994669,11994488,11994285,11994097,
    11993848,11993762,11993603,11993376,11993263,11993003,11992996,11992961,
    11992765,11992691,11992659,11992646,11992631,11992541,11992496,11992485,
    11992414,11992348,11992124,11991983,11991966,11991954,11991932,11991729,
    11991714,11991605,11991453,11991183,11990589,11990392,11990342,11990115,
    11989829,11989402,11989121,11989091,11988919,11988694,11988157,11987968,
    11987500,11987370,11987335,11987173,11987165,11987123,11986991,11986873,
    11986872,11986553,11986381,11986321,11986221,11986103,11986083,11985942,
    11985450,11985118,11984725,11984512,11984441,11984375,11984344,11984020,
    11983757,11983641,11982834,11982507,11982170,11982066,11981754,11981406,
    11981181,11980711,11980289,11980179,11980099,11980073,11979752,11979695,
    11979200,11979181,11979104,11979085,11979056,11978971,11978952,11978930,
    11978621,11978404,11978316,11978227,11977917,11977832,11977696,11977536,
    11977520,11976805,11976561,11976544,11976507,11976318,11976232,11976084,
    11975485,11975307,11975212,11974910,11974166,11972493,11971831,11970989,
    11970533,11968884,11968811,11968686,11968326,11967502,11965422,11965326,
    11965245,11964105,11963726,11963067,11962984,11962834,11962754,11962735,
    11962465,11962458,11962449,11961990,11961808,11961372,11961237,11961032,
    11961021,11958693,11955113,11954276,11954158,11954157,11953977,11953950,
    11953948,11953775,11953595,11953589,11952841,11950748,11950558,11950447,
    11950042,11949839,11949739,11949479,11949261,11946716,11941330,11940803,
    11940449,11940006,11939992,11939880,11939852,11939846,11939546,11939365,
    11937563,11934523,11934441,11933798,11933779,11933757,11926105,11925568,
    11925054,11921172,11918674,11918461,11918117,11917745,11915110,11914802,
    11910092,11909888,11908547,11906542,11906379,11905009,11904843,11903474,
    11903164,11902838,11902830,11901217,11899097,11892187,11891662,11888459,
    11887369,11886901,11886440,11886265,11885785,11884353,11883181,11883146,
    11880594,11878884,11878131,11875613,11871078,11870217,11863686,11862856,
    11855796,11843182,11833330,11832104,11826466,11815070,11804804,11804639,
    11802594,11801206,11789314,11786420,11785874,11785305,11775843,11769286,
    11756278,11753813,11733214
]

placeholders = ','.join(['%s'] * len(dropped_off_ids))

cur.execute(f"""
    SELECT t.id, t.source, t.created,
           -- internal comments = associate notes
           MAX(CASE WHEN tc.is_internal = true THEN tc.comment END) as internal_note,
           -- public comments = customer message
           MAX(CASE WHEN tc.is_internal = false THEN tc.comment END) as public_comment
    FROM support_ticket t
    LEFT JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.id IN ({placeholders})
    GROUP BY t.id, t.source, t.created
    ORDER BY t.source, t.id DESC
""", dropped_off_ids)
rows = cur.fetchall()

io_with_issue   = []  # source=8, has internal note with actual issue
io_empty        = []  # source=8, no internal note either
social_tickets  = []  # source=7
app_empty       = []  # source=1
sage_empty      = []  # source=9

for row in rows:
    tid, source, created, internal_note, public_comment = row
    has_internal = bool(internal_note and len(internal_note.strip()) > 5)

    if source == '8':
        if has_internal:
            io_with_issue.append((tid, created, internal_note[:100] if internal_note else ''))
        else:
            io_empty.append(tid)
    elif source == '7':
        social_tickets.append((tid, created, internal_note[:80] if internal_note else ''))
    elif source == '1':
        app_empty.append(tid)
    elif source == '9':
        sage_empty.append(tid)

print("=" * 78)
print("  DROPPED-OFF LIST — IO TICKET FILTER")
print("=" * 78)
print(f"\n  Total in dropped-off list: {len(rows)}")
print(f"\n  source=8 WhatsApp/IO with ACTUAL issue in internal note : {len(io_with_issue)}  <-- EXCLUDE from auto-close")
print(f"  source=8 WhatsApp/IO truly empty (no internal note)     : {len(io_empty)}  <-- safe to auto-close")
print(f"  source=7 Social/Twitter                                  : {len(social_tickets)}")
print(f"  source=1 App (truly empty)                               : {len(app_empty)}  <-- safe to auto-close")
print(f"  source=9 Sage (truly empty)                              : {len(sage_empty)}  <-- safe to auto-close")

print()
print("=" * 78)
print(f"  IO TICKETS WITH REAL ISSUES — ({len(io_with_issue)}) — DO NOT AUTO-CLOSE")
print("=" * 78)
print(f"\n  {'Ticket ID':<13} {'Date':<13} Associate's Internal Note")
print(f"  {'-'*74}")
for tid, created, note in io_with_issue:
    print(f"  #{tid:<12} {created.strftime('%d %b %Y'):<13} {note}")

ids = [str(r[0]) for r in io_with_issue]
print(f"\n  IDs only:")
for i in range(0, len(ids), 8):
    print('  ' + '   '.join(f'#{x}' for x in ids[i:i+8]))

print()
print("=" * 78)
print(f"  SOCIAL/TWITTER TICKETS — ({len(social_tickets)}) — CHECK SEPARATELY")
print("=" * 78)
for tid, created, note in social_tickets[:10]:
    print(f"  #{tid:<12} {created.strftime('%d %b %Y'):<13} {note or '(no internal note)'}")

print()
print("=" * 78)
print(f"  CLEAN AUTO-CLOSE DROPPED-OFF LIST (excl. IO with issues)")
print(f"  Safe to auto-close: {len(io_empty) + len(app_empty) + len(sage_empty)} tickets")
print("=" * 78)
clean_ids = [str(t) for t in app_empty] + [str(t) for t in io_empty] + [str(t) for t in sage_empty]
clean_ids_int = sorted([int(x) for x in clean_ids], reverse=True)
for i in range(0, len(clean_ids_int), 8):
    print('  ' + '   '.join(f'#{x}' for x in clean_ids_int[i:i+8]))

cur.close()
conn.close()
