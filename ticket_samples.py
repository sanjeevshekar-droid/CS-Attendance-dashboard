import psycopg2, json

conn = psycopg2.connect(
    'postgresql://read_only_user:O811GAqL3k@backend-production-db-cluster.cluster-ro-chqlrp6uyouv.ap-south-1.rds.amazonaws.com:5432/cityflo_final_backend?sslmode=prefer'
)
cur = conn.cursor()

def extract_customer_messages(ticket_id):
    cur.execute("""
        SELECT comment FROM support_ticketcomment
        WHERE ticket_id = %s AND is_internal = false
        ORDER BY created
    """, (ticket_id,))
    customer_msgs = []
    plain_msgs = []
    for row in cur.fetchall():
        try:
            data = json.loads(row[0])
            def find_customer_text(obj, in_yellow=False):
                texts = []
                if isinstance(obj, dict):
                    bg = obj.get('background', '')
                    is_yellow = bg == '#FFEEC0' or in_yellow
                    if obj.get('type') == 'Text' and obj.get('value') and is_yellow:
                        val = obj['value'].strip()
                        if len(val) > 4 and val not in ('AM', 'PM'):
                            texts.append(val)
                    for v in obj.values():
                        if isinstance(v, (dict, list)):
                            texts.extend(find_customer_text(v, is_yellow))
                elif isinstance(obj, list):
                    for item in obj:
                        texts.extend(find_customer_text(item, in_yellow))
                return texts
            customer_msgs.extend(find_customer_text(data))
        except Exception:
            if row[0] and len(row[0]) > 10 and not row[0].startswith('{'):
                plain_msgs.append(row[0][:200])
    return list(dict.fromkeys(customer_msgs + plain_msgs))


def show_category(label, reason, ticket_ids):
    print("=" * 80)
    print("  " + label)
    print("  " + reason)
    print("=" * 80)
    if not ticket_ids:
        print("  (No matching open tickets found in this window)")
    for tid, created in ticket_ids:
        msgs = extract_customer_messages(tid)
        print(f"\n  Ticket #{tid}  |  {created.strftime('%d %b %Y %H:%M')}")
        if msgs:
            for m in msgs[:4]:
                print(f"    Customer: \"{m[:130]}\"")
        else:
            print("    (Menu navigation only — customer did not type a message)")
    print()


# ── CAT 1: REPEATED TICKETS ──────────────────────────────────────────────────
cur.execute("""
    SELECT t.id, t.created FROM support_ticket t
    WHERE t.info_tag = 'Repeated' AND t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '3 days'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 1 — REPEATED TICKETS",
    "Why auto-close: Same customer raised the same issue again. Sage should link to existing open ticket instead of creating a new one.",
    cur.fetchall()
)

# ── CAT 2: ROUTE/SUGGESTION ──────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '14 days'
    AND tc.comment LIKE '%%Suggestions: Route%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 2 — ROUTE / TIMING SUGGESTIONS",
    "Why auto-close: Product feedback only. No CS action possible. Sage logs it and sends acknowledgement automatically.",
    cur.fetchall()
)

# ── CAT 3: AC ISSUE ──────────────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '14 days'
    AND tc.comment LIKE '%%informed the driver to decrease%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 3 — AC ISSUES (Sage already notified driver)",
    "Why auto-close: Sage already sent the driver instruction to adjust AC. CS cannot do more. Close after 24h if no re-escalation.",
    cur.fetchall()
)

# ── CAT 4: SEAT / HARDWARE ───────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND tc.comment LIKE '%%My seat has a problem%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 4 — SEAT / HARDWARE ISSUES",
    "Why auto-close: Acknowledgement sent to customer. Issue forwarded to maintenance. No further CS action needed.",
    cur.fetchall()
)

# ── CAT 5: HYGIENE ───────────────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND tc.comment LIKE '%%Bus Quality and Hygiene issue%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 5 — HYGIENE / BUS CONDITION",
    "Why auto-close: Ops team notified. Same-day fix not possible. Customer already received acknowledgement from Sage.",
    cur.fetchall()
)

# ── CAT 6: TRACKING ──────────────────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND tc.comment LIKE '%%Where is my bus%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 6 — TRACKING COMPLAINTS",
    "Why auto-close: If ride has already ended, tracking issue is resolved. Sage checks ride status and closes automatically.",
    cur.fetchall()
)

# ── CAT 7: LOST & FOUND — manual ─────────────────────────────────────────────
cur.execute("""
    SELECT DISTINCT t.id, t.created FROM support_ticket t
    JOIN support_ticketcomment tc ON tc.ticket_id = t.id
    WHERE t.status = '1' AND t.source = '9'
    AND t.created >= NOW() - INTERVAL '30 days'
    AND tc.comment LIKE '%%I Lost something%%'
    ORDER BY t.id DESC LIMIT 5
""")
show_category(
    "CAT 7 — LOST & FOUND  [DO NOT AUTO-CLOSE — shown for contrast]",
    "Why keep manual: Needs driver/ops to physically locate item. Customer awaiting a tangible outcome — cannot be auto-resolved.",
    cur.fetchall()
)

cur.close()
conn.close()
