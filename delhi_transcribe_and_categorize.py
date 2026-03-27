"""
Delhi Inbound Calls — Transcribe + Auto-Categorize + DB Update
---------------------------------------------------------------
1. Fetches Delhi inbound calls this week with recordings
2. Transcribes via Whisper (skips already-cached)
3. Maps transcript + comments → existing category/subcategory
4. Updates call_category_id + call_sub_category_id in DB

Usage:
    python delhi_transcribe_and_categorize.py                          # transcribe all, dry-run
    python delhi_transcribe_and_categorize.py --commit                 # transcribe + write to DB
    python delhi_transcribe_and_categorize.py --whisper-model small    # better accuracy
    python delhi_transcribe_and_categorize.py --limit 20 --commit      # batch of 20
"""

import argparse
import io
import json
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests
import whisper

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(__file__)
ENV_FILE   = os.path.join(BASE_DIR, "env")
CACHE_FILE = os.path.join(BASE_DIR, "transcripts_cache.json")

_FFMPEG_WINGET = (
    r"C:\Users\User\AppData\Local\Microsoft\WinGet\Packages"
    r"\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe"
    r"\ffmpeg-8.1-full_build\bin"
)
if os.path.isdir(_FFMPEG_WINGET) and _FFMPEG_WINGET not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _FFMPEG_WINGET + os.pathsep + os.environ.get("PATH", "")

WEEK_START_IST = datetime(2026, 3, 24, 0, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
WEEK_START_UTC = WEEK_START_IST.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Category / Subcategory map  (id → name)
# Subcategory → (category_id, subcategory_id)
# ---------------------------------------------------------------------------

# Format: (keywords_in_transcript_or_comment, category_id, subcategory_id)
# Rules are evaluated top-to-bottom; first match wins.
RULES = [
    # ── Missed bus — Cityflo fault ──────────────────────────────────────────
    (["driver didn't stop", "driver did not stop", "bus didn't stop", "bus did not stop",
      "bus did not halt", "didn't halt", "did not halt", "skipped my stop",
      "driver skipped", "missed the stop", "not stop at", "he didn't stop",
      "he did not stop", "stop the shuttle", "stop the bus",
      "driver to stop"],                                                         1, 2),  # Driver didn't stop
    (["left early", "bus left before", "departed before", "left without"],      1, 1),  # Bus left early
    (["breakdown", "broke down", "puncture", "tyre"],                           1, 4),  # Bus breakdown
    (["tracking not", "tracker not", "no tracking", "tracking unavailable",
      "can't see bus", "cant see bus", "not showing", "wrong eta", "eta wrong",
      "wrong location", "incorrect eta"],                                        1, 8),  # Tracking not available
    (["placard", "wrong bus number", "wrong board"],                             1, 15), # Placard missing/wrong
    (["app issue", "pass not visible", "pass not showing", "booking not showing",
      "not reflecting", "app not"],                                              1, 9),  # App issue

    # ── Missed bus — Customer fault ─────────────────────────────────────────
    (["late at", "reached late", "i was late", "i got late", "got delayed",
      "couldn't reach", "could not reach", "traffic on my end"],                2, 5),  # Was late at pickup
    (["wrong stop", "wrong location", "wrong pickup", "standing at wrong",
      "wrong pick up", "incorrect stop"],                                        2, 6),  # Wrong pickup location
    (["forgot to reschedule", "forgot", "didn't reschedule",
      "didn't cancel"],                                                          2, 12), # Forgot to reschedule
    (["was busy", "busy at work", "meeting", "couldn't leave office"],          2, 13), # Was busy at work
    (["wrong bus", "boarded wrong", "took wrong"],                              2, 14), # Boarded wrong bus
    (["unwell", "not feeling well", "feeling sick", "i am sick", "i'm sick"],   2, 16), # Customer unwell
    (["emergency"],                                                              2, 17), # Emergency

    # ── Rescheduling / Cancellation ─────────────────────────────────────────
    (["reschedule", "rescheduled", "rescheduling", "change my bus",
      "change the bus", "book another bus", "shift my ride",
      "late booking", "book late"],                                              3, 18), # Reaching before/after time
    (["cancel", "cancellation", "cancelled", "don't want the ride",
      "want to cancel", "need to cancel"],                                       3, 21), # No more buses / cancel
    (["work from home", "wfh", "on leave", "not going", "holiday",
      "not travelling", "travelling to different", "change in pickup",
      "change pickup", "change drop", "change location"],                        3, 39), # WFH / leave / location change

    # ── Where is my bus ─────────────────────────────────────────────────────
    (["where is my bus", "where is the bus", "how far", "eta",
      "how long", "bus location", "bus still coming", "bus reached",
      "bus on the way", "tracker", "gave update", "update of the bus",
      "status of the bus", "bus status", "bus coming",
      "unable to track", "no bus", "waiting at", "there is no bus",
      "it is not showing", "not showing"],                                       4, 23), # Tracking active

    # ── Lost & Found ────────────────────────────────────────────────────────
    (["lost", "forgot my", "left my", "missing item", "found"],                 5, 27), # Low value object

    # ── Route enquiry ───────────────────────────────────────────────────────
    (["subscription", "ride pack", "pass", "membership", "plan"],               6, 42), # Subscription inquiry
    (["route", "new route", "route available", "do you have",
      "bus available", "service available", "operate",
      "timings", "schedule", "stop available"],                                  6, 31), # Route we operate

    # ── Refund ──────────────────────────────────────────────────────────────
    (["refund", "money back", "money deducted", "amount deducted",
      "payment deducted", "charged extra", "wrong charge",
      "not received refund", "where is my refund",
      "want my money back", "credit to bank", "bank account",
      "cityflo wallet", "got deducted", "deducted"],                            8, 33), # Cancellation policy / refund

    # ── Bus quality ─────────────────────────────────────────────────────────
    (["ac not working", "ac issue", "no ac", "air conditioning"],               9, 36), # AC issue
    (["water", "leakage", "wet seat"],                                          9, 37), # Water leakage
    (["breakdown midway", "broke down midway", "stopped midway"],               9, 35), # Breakdown midway
]

CAT_NAMES = {
    1: "I've missed my bus (Due to Cityflo)",
    2: "I've missed my bus (Due to Customer)",
    3: "Rescheduling / Cancellation request",
    4: "Where is my bus?",
    5: "Lost & Found",
    6: "Route enquiry",
    7: "Rental Query",
    8: "I have not got my refund",
    9: "Bus quality",
}

SUBCAT_NAMES = {
    1: "Bus left early", 2: "Driver didn't stop", 3: "Bus didn't stop at correct location",
    4: "Bus breakdown", 5: "Was late at pick up location", 6: "Standing at incorrect pick up location",
    7: "Bus was late", 8: "Tracking not available", 9: "app issue", 10: "Admin initiated rescheduling",
    11: "No booking", 12: "Forgot to reschedule", 13: "Was busy at work", 14: "Boarded the wrong bus",
    15: "Placard missing/wrong", 16: "Customer unwell", 17: "Emergency situation",
    18: "Reaching pickup before/after time", 19: "Bus is late", 20: "Pass dates not correct",
    21: "No more buses on that route", 22: "Timing not suitable", 23: "Tracking Active (gave location)",
    24: "Driver phone issue", 25: "Fleet X issue", 26: "Tech. issue",
    27: "Low value object", 28: "High value object", 29: "Fresh inquiry", 30: "Repeat call",
    31: "Route we currently operate", 32: "Route we don't operate", 33: "Cancellation policy explained",
    34: "Cancellation before 10 min window", 35: "Breakdown Midway", 36: "AC issue",
    37: "Water leakage", 38: "Others", 39: "Work from home / on leave / location change",
    40: "Refund already done", 41: "Change in Pick up / Drop location", 42: "Subscription Inquiry",
    43: "Lost & Found Others",
}


def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def is_garbage_transcript(text):
    """Return True if Whisper hallucinated junk (common on short/Hindi calls with base model)."""
    if not text or len(text.strip()) < 10:
        return True
    # Flag if > 30% of chars are non-ASCII (Arabic, Devanagari, CJK etc. in an 'English' transcript)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    if non_ascii / len(text) > 0.30:
        return True
    # Flag if it looks like pure hallucination phrases Whisper emits on silence/noise
    hallucination_phrases = [
        "thank you for watching", "please subscribe", "thank you for listening",
        "top 10 video", "link to video description", "workers' parade",
        "i know. i know", "music music music",
    ]
    lower = text.lower()
    if any(p in lower for p in hallucination_phrases):
        return True
    return False


def classify(transcript, comment):
    # Only use transcript if it looks valid
    tx = "" if is_garbage_transcript(transcript) else (transcript or "")
    text = (tx + " " + (comment or "")).lower()
    if not text.strip():
        return None, None
    for keywords, cat_id, subcat_id in RULES:
        for kw in keywords:
            if kw in text:
                return cat_id, subcat_id
    return None, None


QUERY = """
SELECT sc.id, sc.voice_recording_url, sc.call_duration, sc.call_status,
       sc.call_comments, sc.call_category_id, sc.call_sub_category_id,
       ep.first_name || ' ' || ep.last_name AS agent_name,
       cp.first_name || ' ' || cp.last_name AS customer_name
FROM support_customercalls sc
LEFT JOIN users_employee ue ON sc.employee_id = ue.id
LEFT JOIN users_person   ep ON ue.person_id    = ep.id
LEFT JOIN users_customer uc ON sc.customer_id  = uc.id
LEFT JOIN users_person   cp ON uc.person_id    = cp.id
LEFT JOIN geo_city gc ON sc.city_id = gc.id
WHERE sc.call_initiate_time >= %(week_start)s
  AND sc.call_type = 'incoming'
  AND LOWER(gc.name) LIKE '%%delhi%%'
  AND sc.voice_recording_url IS NOT NULL
  AND sc.voice_recording_url != ''
ORDER BY sc.call_initiate_time
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit",         action="store_true", help="Write categories to DB")
    parser.add_argument("--whisper-model",  default="base",      help="Whisper model (base/small/medium)")
    parser.add_argument("--limit",          type=int, default=0, help="Max recordings to transcribe (0=all)")
    args = parser.parse_args()

    env   = load_env(ENV_FILE)
    conn  = psycopg2.connect(env["DATABASE_URL"])
    cache = load_cache()
    auth  = (env["EXOTEL_API_KEY"], env["EXOTEL_API_TOKEN"])

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(QUERY, {"week_start": WEEK_START_UTC})
        rows = cur.fetchall()

    print(f"\n{'='*65}")
    print(f"  Delhi Inbound Calls — Transcribe & Categorize")
    print(f"{'='*65}")
    print(f"  Total calls with recordings : {len(rows)}")

    # Split cached vs uncached
    # Skip if already attempted (even if empty/error) — avoids re-doing Hindi calls
    to_transcribe = [r for r in rows if str(r["id"]) not in cache]
    already_cached = [r for r in rows if str(r["id"]) in cache]
    print(f"  Already transcribed         : {len(already_cached)}")
    print(f"  Need transcription          : {len(to_transcribe)}")
    if args.limit:
        to_transcribe = to_transcribe[:args.limit]
        print(f"  Batch limit applied         : {args.limit}")
    print()

    # ---------------------------------------------------------------------------
    # Transcribe
    # ---------------------------------------------------------------------------
    if to_transcribe:
        print(f"  Loading Whisper model '{args.whisper_model}'...", end=" ", flush=True)
        model = whisper.load_model(args.whisper_model)
        print("OK\n")

        for i, row in enumerate(to_transcribe, 1):
            call_id = str(row["id"])
            url     = row["voice_recording_url"]
            dur     = row["call_duration"] or 0
            mm, ss  = divmod(dur, 60)
            print(f"  [{i:>3}/{len(to_transcribe)}] #{call_id}  {mm}m{ss:02d}s  ", end="", flush=True)

            tmp_path = None
            trimmed_path = None
            try:
                resp = requests.get(url, timeout=(10, 60), auth=auth, stream=True)
                resp.raise_for_status()

                ct = resp.headers.get("content-type", "")
                ext = ".mp3"
                if "wav" in ct or url.endswith(".wav"):   ext = ".wav"
                elif "ogg" in ct or url.endswith(".ogg"): ext = ".ogg"

                with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                    for chunk in resp.iter_content(chunk_size=65536):
                        tmp.write(chunk)
                    tmp_path = tmp.name

                # Trim to first 60s with ffmpeg so Whisper runs fast
                import subprocess
                trimmed_path = tmp_path.replace(ext, f"_60s{ext}")
                subprocess.run(
                    ["ffmpeg", "-y", "-i", tmp_path, "-t", "60", "-c", "copy", trimmed_path],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                transcribe_src = trimmed_path if os.path.exists(trimmed_path) else tmp_path

                result = model.transcribe(transcribe_src)
                text   = result["text"].strip()
                lang   = result.get("language", "")
                cache[call_id] = {
                    "text": text, "language": lang,
                    "transcribed_at": datetime.now().isoformat(),
                }
                print(f"OK  ({len(text)} chars, lang={lang or '?'})")

            except requests.HTTPError as e:
                code = e.response.status_code if e.response else "?"
                cache[call_id] = {"text": None, "error": f"HTTP {code}", "transcribed_at": datetime.now().isoformat()}
                print(f"HTTP {code}")
            except Exception as e:
                cache[call_id] = {"text": None, "error": str(e), "transcribed_at": datetime.now().isoformat()}
                print(f"ERR: {e}")
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                if trimmed_path and os.path.exists(trimmed_path):
                    os.unlink(trimmed_path)

        save_cache(cache)
        print(f"\n  Cache saved → {CACHE_FILE}\n")

    # ---------------------------------------------------------------------------
    # Classify all rows and collect DB updates
    # ---------------------------------------------------------------------------
    print(f"{'='*65}")
    print("  CATEGORIZATION RESULTS")
    print(f"{'='*65}\n")

    updates        = []  # (call_id, cat_id, subcat_id)
    cat_summary    = defaultdict(int)
    no_match       = []

    for row in rows:
        call_id = str(row["id"])
        tx      = cache.get(call_id, {})
        text    = tx.get("text") if tx else None
        cat_id, subcat_id = classify(text, row["call_comments"])

        if cat_id:
            updates.append((row["id"], cat_id, subcat_id))
            label = f"{CAT_NAMES[cat_id]} > {SUBCAT_NAMES[subcat_id]}"
            cat_summary[label] += 1
            dur = row["call_duration"] or 0
            mm, ss = divmod(dur, 60)
            print(f"  #{call_id:<10} {mm}m{ss:02d}s  {row['agent_name'] or 'Unassigned':<22} → {label}")
        else:
            no_match.append(row)

    print(f"\n  {len(no_match)} call(s) could not be categorized (insufficient transcript / short call)")

    if no_match:
        print("  Uncategorized:")
        for r in no_match:
            cid = str(r["id"])
            tx  = cache.get(cid, {})
            snippet = (tx.get("text") or "")[:80].replace("\n", " ")
            print(f"    #{r['id']:<10} comment='{r['call_comments'] or ''}' | tx='{snippet}'")

    # Summary
    print(f"\n{'='*65}")
    print("  CATEGORY SUMMARY")
    print(f"{'='*65}")
    for label, cnt in sorted(cat_summary.items(), key=lambda x: -x[1]):
        print(f"  {cnt:>4}  {label}")

    # ---------------------------------------------------------------------------
    # DB update
    # ---------------------------------------------------------------------------
    print(f"\n  Total to update: {len(updates)}")

    if not args.commit:
        print("\n  [DRY RUN] Pass --commit to write to DB.\n")
        conn.close()
        return

    print("\n  Writing to database...", end=" ", flush=True)
    updated = 0
    with conn.cursor() as cur:
        for call_id, cat_id, subcat_id in updates:
            cur.execute(
                """UPDATE support_customercalls
                   SET call_category_id = %s, call_sub_category_id = %s
                   WHERE id = %s""",
                (cat_id, subcat_id, call_id),
            )
            updated += cur.rowcount
    conn.commit()
    conn.close()
    print(f"Done. {updated} row(s) updated.\n")


if __name__ == "__main__":
    main()
