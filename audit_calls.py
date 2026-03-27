"""
Cityflo Customer Call Transcription Audit
------------------------------------------
Fetches call records for the past N days and prints a per-agent audit report.
Optionally transcribes voice recordings using OpenAI Whisper.

Usage:
    python audit_calls.py                                   # past 1 day, plain text
    python audit_calls.py --days 7 --output report.html    # HTML report, 7 days
    python audit_calls.py --days 1 --transcribe            # transcribe up to 20 recordings
    python audit_calls.py --days 7 --transcribe --transcribe-limit 100 --whisper-model small
"""

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests
from tabulate import tabulate

# Ensure ffmpeg (installed via winget) is on PATH for Whisper
_FFMPEG_WINGET = (
    r"C:\Users\User\AppData\Local\Microsoft\WinGet\Packages"
    r"\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe"
    r"\ffmpeg-8.1-full_build\bin"
)
if os.path.isdir(_FFMPEG_WINGET) and _FFMPEG_WINGET not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _FFMPEG_WINGET + os.pathsep + os.environ.get("PATH", "")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENV_FILE        = os.path.join(os.path.dirname(__file__), "env")
CACHE_FILE      = os.path.join(os.path.dirname(__file__), "transcripts_cache.json")
DEFAULT_DAYS    = 1
DEFAULT_LIMIT   = 20
DEFAULT_MODEL   = "base"


def load_env(path: str) -> dict:
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    return env


# ---------------------------------------------------------------------------
# Database query
# ---------------------------------------------------------------------------

QUERY = """
SELECT
    sc.id,
    sc.call_initiate_time,
    sc.call_type,
    sc.call_status,
    sc.call_duration,
    sc.callers_call_duration,
    sc.call_issue,
    sc.call_reason,
    sc.call_comments,
    sc.voice_recording_url,
    sc.follow_up,
    sc.call_back,
    sc.priority,
    sc.caller_number,
    ep.first_name || ' ' || ep.last_name        AS agent_name,
    ue.email                                     AS agent_email,
    ue.id                                        AS agent_id,
    cp.first_name || ' ' || cp.last_name         AS customer_name,
    uc.email                                     AS customer_email,
    cat.name                                     AS call_category,
    subcat.name                                  AS call_subcategory,
    reason.name                                  AS reason_detail,
    gc.name                                      AS city
FROM support_customercalls sc
LEFT JOIN users_employee ue       ON sc.employee_id         = ue.id
LEFT JOIN users_person   ep       ON ue.person_id            = ep.id
LEFT JOIN users_customer uc       ON sc.customer_id          = uc.id
LEFT JOIN users_person   cp       ON uc.person_id            = cp.id
LEFT JOIN support_customercallscategory    cat    ON sc.call_category_id     = cat.id
LEFT JOIN support_customercallssubcategory subcat ON sc.call_sub_category_id = subcat.id
LEFT JOIN support_customercallreason       reason ON sc.reason_id            = reason.id
LEFT JOIN geo_city gc             ON sc.city_id              = gc.id
WHERE sc.call_initiate_time >= NOW() AT TIME ZONE 'UTC' - INTERVAL %(interval)s
  AND sc.call_type IN ('incoming', 'outgoing')
  AND (sc.voice_recording_url IS NULL OR sc.voice_recording_url != '')
ORDER BY agent_id NULLS LAST, sc.call_initiate_time
"""


def fetch_calls(conn, days: int) -> list[dict]:
    interval = f"{days} days"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(QUERY, {"interval": interval})
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def transcribe_recordings(rows: list[dict], limit: int, model_name: str, auth: tuple | None = None, max_duration: int | None = None) -> dict:
    """
    Downloads and transcribes voice recordings using Whisper.
    Skips calls already present in the local cache.
    Returns the full cache dict: {call_id -> {text, language, transcribed_at, error?}}
    """
    import whisper

    cache = load_cache()

    eligible = [
        r for r in rows
        if r["voice_recording_url"]
        and str(r["id"]) not in cache
        and (max_duration is None or (r["call_duration"] or 0) <= max_duration)
    ]
    to_transcribe = eligible[:limit]

    already_cached = sum(1 for r in rows if r["voice_recording_url"] and str(r["id"]) in cache)
    skipped_long   = sum(
        1 for r in rows
        if r["voice_recording_url"]
        and str(r["id"]) not in cache
        and max_duration is not None
        and (r["call_duration"] or 0) > max_duration
    )

    if already_cached:
        print(f"  {already_cached} recording(s) loaded from cache.")
    if skipped_long:
        print(f"  {skipped_long} recording(s) skipped (duration > {max_duration}s / {max_duration//60}m {max_duration%60:02d}s).")

    if not to_transcribe:
        print("  Nothing new to transcribe.")
        return cache

    print(f"  Loading Whisper model '{model_name}'...", end=" ", flush=True)
    model = whisper.load_model(model_name)
    print("OK")
    print(f"  Transcribing {len(to_transcribe)} recording(s) (limit={limit})...\n")

    tmp_path = None
    for i, row in enumerate(to_transcribe, 1):
        call_id = str(row["id"])
        url     = row["voice_recording_url"]
        print(f"  [{i:>3}/{len(to_transcribe)}] Call #{call_id}  ", end="", flush=True)

        try:
            resp = requests.get(url, timeout=(10, 30), auth=auth, stream=True)
            resp.raise_for_status()

            content_type = resp.headers.get("content-type", "")
            if   "wav"  in content_type or url.endswith(".wav"):  ext = ".wav"
            elif "ogg"  in content_type or url.endswith(".ogg"):  ext = ".ogg"
            elif "flac" in content_type or url.endswith(".flac"): ext = ".flac"
            else:                                                  ext = ".mp3"

            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                for chunk in resp.iter_content(chunk_size=65536):
                    tmp.write(chunk)
                tmp_path = tmp.name

            result  = model.transcribe(tmp_path)
            text    = result["text"].strip()
            lang    = result.get("language", "")

            cache[call_id] = {
                "text":           text,
                "language":       lang,
                "transcribed_at": datetime.now().isoformat(),
            }
            print(f"OK  ({len(text)} chars, lang={lang or '?'})")

        except requests.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            msg  = f"HTTP {code}"
            cache[call_id] = {"text": None, "error": msg, "transcribed_at": datetime.now().isoformat()}
            print(msg)

        except requests.exceptions.ConnectionError as e:
            msg = f"Connection error: {e}"
            cache[call_id] = {"text": None, "error": msg, "transcribed_at": datetime.now().isoformat()}
            print(msg)

        except requests.exceptions.Timeout:
            cache[call_id] = {"text": None, "error": "Timeout", "transcribed_at": datetime.now().isoformat()}
            print("Timeout")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            cache[call_id] = {"text": None, "error": msg, "transcribed_at": datetime.now().isoformat()}
            print(msg)

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
                tmp_path = None

    save_cache(cache)
    print(f"\n  Cache saved -> {CACHE_FILE}")
    return cache


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_duration(seconds) -> str:
    if seconds is None:
        return "-"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s"


def fmt_time(ts) -> str:
    if ts is None:
        return "-"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ist = ts + timedelta(seconds=19800)
    return ist.strftime("%d %b %Y  %H:%M IST")


def trunc(text, width=40) -> str:
    if not text:
        return "-"
    text = str(text).strip().replace("\n", " ")
    return text[:width] + "…" if len(text) > width else text


def priority_label(p) -> str:
    if p is None:
        return "-"
    return {
        "H": "High", "M": "Medium", "L": "Low", "U": "Urgent",
        "1": "Low",  "2": "Medium", "3": "High", "4": "Urgent",
    }.get(str(p), str(p))


# ---------------------------------------------------------------------------
# Plain-text report
# ---------------------------------------------------------------------------

def build_report(rows: list[dict], days: int) -> str:
    lines = []
    lines.append("=" * 80)
    lines.append(f"  CITYFLO  |  Customer Call Audit  |  Past {days} day{'s' if days != 1 else ''}")
    lines.append(f"  Generated: {datetime.now().strftime('%d %b %Y  %H:%M')}")
    lines.append("=" * 80)

    if not rows:
        lines.append("\n  No call records found for this period.\n")
        return "\n".join(lines)

    agents: dict[str, list] = {}
    for row in rows:
        key = row["agent_name"] or "Unassigned"
        agents.setdefault(key, []).append(row)

    overall_total = overall_answered = overall_duration = 0

    for agent_name, calls in agents.items():
        agent_email = calls[0]["agent_email"] or ""
        total      = len(calls)
        answered   = sum(1 for c in calls if (c["call_status"] or "").lower() == "answered")
        missed     = sum(1 for c in calls if (c["call_status"] or "").lower() == "missed")
        inbound    = sum(1 for c in calls if (c["call_type"] or "").lower() == "incoming")
        outbound   = sum(1 for c in calls if (c["call_type"] or "").lower() == "outgoing")
        follow_ups = sum(1 for c in calls if c["follow_up"])
        callbacks  = sum(1 for c in calls if c["call_back"])
        durations  = [c["call_duration"] for c in calls if c["call_duration"]]
        avg_dur    = int(sum(durations) / len(durations)) if durations else 0
        total_dur  = sum(durations) if durations else 0
        recordings = sum(1 for c in calls if c["voice_recording_url"])

        overall_total    += total
        overall_answered += answered
        overall_duration += total_dur

        lines.append("")
        lines.append(f"  AGENT: {agent_name}  ({agent_email})")
        lines.append("  " + "-" * 76)
        lines.append(tabulate([
            ["Total Calls", total,      "Answered",       answered,              "Missed",     missed],
            ["Incoming",    inbound,    "Outgoing",       outbound,              "Follow-ups", follow_ups],
            ["Avg Duration",fmt_duration(avg_dur),"Total Duration",fmt_duration(total_dur),"Recordings",recordings],
            ["Callbacks",   callbacks,  "",               "",                    "",           ""],
        ], tablefmt="plain"))
        lines.append("")

        detail_rows = []
        for c in calls:
            detail_rows.append([
                fmt_time(c["call_initiate_time"]),
                trunc(c["customer_name"], 20),
                (c["call_type"] or "-").capitalize(),
                (c["call_status"] or "-").capitalize(),
                fmt_duration(c["call_duration"]),
                trunc(c["call_category"], 18),
                trunc(c["call_subcategory"], 22),
                trunc(c["call_issue"], 20),
                priority_label(c["priority"]),
                "Yes" if c["follow_up"] else "No",
                trunc(c["call_comments"], 45),
                "[Y]" if c["voice_recording_url"] else "-",
            ])

        lines.append(tabulate(detail_rows, headers=[
            "Time (IST)", "Customer", "Type", "Status", "Duration",
            "Category", "Subcategory", "Issue", "Priority",
            "Follow-up", "Comments", "Rec",
        ], tablefmt="simple"))

        rec_calls = [c for c in calls if c["voice_recording_url"]]
        if rec_calls:
            lines.append("")
            lines.append("  Recording URLs:")
            for c in rec_calls:
                lines.append(f"    [{c['id']}] {fmt_time(c['call_initiate_time'])}  ->  {c['voice_recording_url']}")

        lines.append("")

    lines.append("=" * 80)
    lines.append("  OVERALL SUMMARY")
    lines.append("-" * 80)
    lines.append(tabulate([
        ["Total Calls",    overall_total],
        ["Total Answered", overall_answered],
        ["Total Duration", fmt_duration(overall_duration)],
        ["Agents Active",  len(agents)],
    ], tablefmt="plain"))
    lines.append("=" * 80)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML report helpers
# ---------------------------------------------------------------------------

def esc(val) -> str:
    import html
    if val is None:
        return ""
    return html.escape(str(val))


def status_badge(status: str) -> str:
    s = (status or "").lower()
    color = {
        "finished": "#22c55e", "answered": "#22c55e",
        "missed":   "#ef4444", "busy":     "#f97316",
        "initiated":"#6b7280", "picked":   "#3b82f6",
    }.get(s, "#9ca3af")
    return f'<span class="badge" style="background:{color}">{esc((status or "-").capitalize())}</span>'


def type_badge(call_type: str) -> str:
    t = (call_type or "").lower()
    color = "#3b82f6" if t == "incoming" else "#8b5cf6"
    arrow = "&#8594;" if t == "outgoing" else "&#8592;"
    return f'<span class="badge" style="background:{color}">{arrow} {esc((call_type or "-").capitalize())}</span>'


def priority_badge(p) -> str:
    label = priority_label(p)
    color = {"Low":"#6b7280","Medium":"#f59e0b","High":"#ef4444","Urgent":"#7c3aed"}.get(label,"#6b7280")
    return f'<span class="badge" style="background:{color}">{esc(label)}</span>'


# ---------------------------------------------------------------------------
# HTML report builder
# ---------------------------------------------------------------------------

def build_html_report(rows: list[dict], days: int, transcripts: dict | None = None) -> str:
    transcripts = transcripts or {}
    generated   = datetime.now().strftime("%d %b %Y %H:%M")
    period      = f"Past {days} day{'s' if days != 1 else ''}"

    agents: dict[str, list] = {}
    for row in rows:
        agents.setdefault(row["agent_name"] or "Unassigned", []).append(row)

    overall_total = overall_finished = overall_missed = 0
    overall_duration = overall_followup = overall_recs = 0
    agent_blocks = []

    for agent_name, calls in agents.items():
        agent_email = calls[0]["agent_email"] or ""
        total       = len(calls)
        finished    = sum(1 for c in calls if (c["call_status"] or "").lower() == "finished")
        missed      = sum(1 for c in calls if (c["call_status"] or "").lower() == "missed")
        incoming    = sum(1 for c in calls if (c["call_type"] or "").lower() == "incoming")
        outgoing    = sum(1 for c in calls if (c["call_type"] or "").lower() == "outgoing")
        follow_ups  = sum(1 for c in calls if c["follow_up"])
        durations   = [c["call_duration"] for c in calls if c["call_duration"]]
        avg_dur     = int(sum(durations) / len(durations)) if durations else 0
        total_dur   = sum(durations) if durations else 0
        recordings  = sum(1 for c in calls if c["voice_recording_url"])

        overall_total    += total
        overall_finished += finished
        overall_missed   += missed
        overall_duration += total_dur
        overall_followup += follow_ups
        overall_recs     += recordings

        initials = "".join(p[0].upper() for p in agent_name.split() if p)[:2]

        # ---- per-call rows ------------------------------------------------
        call_rows_html = []
        for c in calls:
            call_id   = str(c["id"])
            comment   = esc(str(c["call_comments"] or "").strip().replace("\n", " "))
            tr_data   = transcripts.get(call_id, {})
            tr_text   = tr_data.get("text")
            tr_error  = tr_data.get("error")
            tr_lang   = (tr_data.get("language") or "").upper()

            # Recording cell
            if c["voice_recording_url"]:
                rec_html = f'<a href="{esc(c["voice_recording_url"])}" target="_blank" class="rec-link">&#9654; Play</a>'
                if tr_text:
                    rec_html += f'<br><button class="btn-tr" onclick="toggleTr(\'tr-{call_id}\')">&#128196; Transcript</button>'
                elif tr_error:
                    rec_html += f'<br><span class="tr-err" title="{esc(tr_error)}">&#10060; {esc(tr_error)}</span>'
            else:
                rec_html = '<span style="color:#6b7280">—</span>'

            # Main data row
            call_rows_html.append(f"""
            <tr>
              <td class="nowrap">{esc(fmt_time(c["call_initiate_time"]))}</td>
              <td>{esc(c["customer_name"] or "—")}</td>
              <td>{type_badge(c["call_type"])}</td>
              <td>{status_badge(c["call_status"])}</td>
              <td class="nowrap">{esc(fmt_duration(c["call_duration"]))}</td>
              <td>{esc(c["call_category"] or "—")}</td>
              <td>{esc(c["call_subcategory"] or "—")}</td>
              <td>{esc(c["call_issue"] or "—")}</td>
              <td>{priority_badge(c["priority"])}</td>
              <td class="center">{"&#10003;" if c["follow_up"] else "—"}</td>
              <td class="comment" title="{comment}">{(comment[:80] + "…") if len(comment) > 80 else comment or "—"}</td>
              <td class="rec-cell">{rec_html}</td>
            </tr>""")

            # Transcript sub-row (hidden until toggled)
            if tr_text:
                lang_badge = f'<span class="lang-badge">{esc(tr_lang)}</span>' if tr_lang else ""
                call_rows_html.append(f"""
            <tr class="tr-row" id="tr-{call_id}">
              <td colspan="12">
                <div class="tr-box">
                  <div class="tr-header">
                    <span>&#127908; Whisper Transcript</span>
                    {lang_badge}
                    <span class="tr-meta">Call #{call_id} &middot; {esc(fmt_time(c["call_initiate_time"]))}</span>
                  </div>
                  <div class="tr-text">{esc(tr_text)}</div>
                </div>
              </td>
            </tr>""")

        # ---- agent card ---------------------------------------------------
        slug = agent_name.replace(" ", "_").lower()
        agent_blocks.append(f"""
        <div class="agent-card">
          <div class="agent-header" onclick="toggleAgent('{slug}')">
            <div class="agent-avatar">{esc(initials)}</div>
            <div class="agent-info">
              <div class="agent-name">{esc(agent_name)}</div>
              <div class="agent-email">{esc(agent_email)}</div>
            </div>
            <div class="agent-stats">
              <div class="stat"><span class="stat-val">{total}</span><span class="stat-lbl">Total</span></div>
              <div class="stat"><span class="stat-val green">{finished}</span><span class="stat-lbl">Finished</span></div>
              <div class="stat"><span class="stat-val red">{missed}</span><span class="stat-lbl">Missed</span></div>
              <div class="stat"><span class="stat-val blue">{incoming}</span><span class="stat-lbl">Incoming</span></div>
              <div class="stat"><span class="stat-val purple">{outgoing}</span><span class="stat-lbl">Outgoing</span></div>
              <div class="stat"><span class="stat-val amber">{follow_ups}</span><span class="stat-lbl">Follow-ups</span></div>
              <div class="stat"><span class="stat-val">{fmt_duration(avg_dur)}</span><span class="stat-lbl">Avg Dur</span></div>
              <div class="stat"><span class="stat-val">{fmt_duration(total_dur)}</span><span class="stat-lbl">Total Dur</span></div>
              <div class="stat"><span class="stat-val">{recordings}</span><span class="stat-lbl">Recordings</span></div>
            </div>
            <div class="chevron" id="chev-{slug}">&#9660;</div>
          </div>
          <div class="agent-body" id="body-{slug}">
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Time (IST)</th><th>Customer</th><th>Type</th><th>Status</th>
                    <th>Duration</th><th>Category</th><th>Subcategory</th><th>Issue</th>
                    <th>Priority</th><th>Follow-up</th><th>Comments</th><th>Recording</th>
                  </tr>
                </thead>
                <tbody>{''.join(call_rows_html)}</tbody>
              </table>
            </div>
          </div>
        </div>""")

    # ---- overall summary --------------------------------------------------
    summary_html = f"""
    <div class="summary-bar">
      <div class="sum-card"><div class="sum-val">{overall_total}</div><div class="sum-lbl">Total Calls</div></div>
      <div class="sum-card"><div class="sum-val green">{overall_finished}</div><div class="sum-lbl">Finished</div></div>
      <div class="sum-card"><div class="sum-val red">{overall_missed}</div><div class="sum-lbl">Missed</div></div>
      <div class="sum-card"><div class="sum-val amber">{overall_followup}</div><div class="sum-lbl">Follow-ups</div></div>
      <div class="sum-card"><div class="sum-val blue">{overall_recs}</div><div class="sum-lbl">Recordings</div></div>
      <div class="sum-card"><div class="sum-val">{fmt_duration(overall_duration)}</div><div class="sum-lbl">Total Duration</div></div>
      <div class="sum-card"><div class="sum-val purple">{len(agents)}</div><div class="sum-lbl">Agents</div></div>
    </div>"""

    no_data = (
        "<p style='text-align:center;color:#6b7280;padding:3rem'>"
        "No agent-handled calls found for this period.</p>"
        if not rows else ""
    )

    # ---- full HTML --------------------------------------------------------
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cityflo Call Audit — {esc(period)}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #0f172a; color: #e2e8f0; font-size: 13px; }}
  a {{ color: inherit; text-decoration: none; }}

  /* ---- Header ---- */
  .top-bar {{ background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%);
              border-bottom: 1px solid #1e40af; padding: 1.2rem 2rem;
              display: flex; align-items: center; justify-content: space-between; }}
  .top-bar h1 {{ font-size: 1.3rem; font-weight: 700; color: #fff; }}
  .top-bar h1 span {{ color: #60a5fa; }}
  .top-meta {{ font-size: 11px; color: #94a3b8; text-align: right; }}
  .top-meta strong {{ color: #cbd5e1; }}

  /* ---- Summary bar ---- */
  .summary-bar {{ display: flex; flex-wrap: wrap; gap: .75rem;
                  padding: 1.25rem 2rem; background: #0f172a; }}
  .sum-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px;
               padding: .8rem 1.2rem; min-width: 110px; text-align: center;
               flex: 1; transition: transform .15s; }}
  .sum-card:hover {{ transform: translateY(-2px); }}
  .sum-val {{ font-size: 1.5rem; font-weight: 700; color: #f1f5f9; }}
  .sum-lbl {{ font-size: 10px; color: #64748b; text-transform: uppercase;
              letter-spacing: .8px; margin-top: 2px; }}

  /* ---- Filter bar ---- */
  .filter-bar {{ padding: .75rem 2rem; background: #0f172a;
                 display: flex; gap: .75rem; align-items: center; flex-wrap: wrap; }}
  .filter-bar input {{ background: #1e293b; border: 1px solid #334155; color: #e2e8f0;
                       border-radius: 8px; padding: .4rem .8rem; font-size: 12px;
                       outline: none; width: 240px; }}
  .filter-bar input:focus {{ border-color: #3b82f6; }}
  .filter-bar label {{ color: #64748b; font-size: 11px; }}
  .btn-toggle {{ background: #1e293b; border: 1px solid #334155; color: #94a3b8;
                 border-radius: 8px; padding: .4rem .9rem; font-size: 11px;
                 cursor: pointer; transition: background .15s; }}
  .btn-toggle:hover {{ background: #334155; color: #e2e8f0; }}

  /* ---- Agent cards ---- */
  .agents {{ padding: 0 2rem 2rem; }}
  .agent-card {{ background: #1e293b; border: 1px solid #334155;
                 border-radius: 12px; margin-bottom: 1rem; overflow: hidden; }}
  .agent-header {{ display: flex; align-items: center; gap: 1rem;
                   padding: .9rem 1.25rem; cursor: pointer;
                   transition: background .15s; user-select: none; }}
  .agent-header:hover {{ background: #263548; }}
  .agent-avatar {{ width: 40px; height: 40px; border-radius: 50%;
                   background: linear-gradient(135deg, #3b82f6, #8b5cf6);
                   display: flex; align-items: center; justify-content: center;
                   font-weight: 700; font-size: .9rem; color: #fff; flex-shrink: 0; }}
  .agent-info {{ min-width: 160px; }}
  .agent-name {{ font-weight: 600; color: #f1f5f9; font-size: .9rem; }}
  .agent-email {{ font-size: 11px; color: #64748b; margin-top: 1px; }}
  .agent-stats {{ display: flex; flex-wrap: wrap; gap: .5rem; flex: 1; }}
  .stat {{ background: #0f172a; border-radius: 6px; padding: .3rem .6rem; text-align: center; min-width: 60px; }}
  .stat-val {{ display: block; font-weight: 700; font-size: .85rem; color: #e2e8f0; }}
  .stat-lbl {{ font-size: 9px; color: #475569; text-transform: uppercase; letter-spacing: .5px; }}
  .chevron {{ color: #475569; font-size: .8rem; flex-shrink: 0; transition: transform .2s; }}
  .chevron.open {{ transform: rotate(180deg); }}

  /* ---- Table ---- */
  .agent-body {{ display: none; border-top: 1px solid #334155; }}
  .agent-body.open {{ display: block; }}
  .table-wrap {{ overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  thead tr {{ background: #0f172a; }}
  th {{ padding: .55rem .75rem; text-align: left; color: #64748b; font-weight: 600;
        text-transform: uppercase; font-size: 10px; letter-spacing: .6px;
        white-space: nowrap; border-bottom: 1px solid #1e293b; }}
  td {{ padding: .5rem .75rem; border-bottom: 1px solid #1a2535; vertical-align: top; color: #cbd5e1; }}
  tbody tr:last-child td {{ border-bottom: none; }}
  tbody tr:not(.tr-row):hover {{ background: #1a2a3a; }}
  .nowrap {{ white-space: nowrap; }}
  .center {{ text-align: center; }}
  .comment {{ max-width: 280px; color: #94a3b8; }}
  .rec-cell {{ white-space: nowrap; }}

  /* ---- Badges ---- */
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px;
            font-size: 10px; font-weight: 600; color: #fff; white-space: nowrap; }}
  .rec-link {{ display: inline-block; background: #1d4ed8; color: #bfdbfe;
               padding: 2px 8px; border-radius: 6px; font-size: 10px;
               font-weight: 600; transition: background .15s; }}
  .rec-link:hover {{ background: #2563eb; }}

  /* ---- Transcript button ---- */
  .btn-tr {{ background: #0f3460; border: 1px solid #1e40af; color: #93c5fd;
             border-radius: 6px; padding: 2px 8px; font-size: 10px; font-weight: 600;
             cursor: pointer; margin-top: 4px; transition: background .15s; }}
  .btn-tr:hover {{ background: #1e40af; color: #dbeafe; }}
  .tr-err {{ font-size: 10px; color: #f87171; }}

  /* ---- Transcript sub-row ---- */
  .tr-row {{ display: none; }}
  .tr-row.open {{ display: table-row; }}
  .tr-box {{ background: #0d1f35; border: 1px solid #1e3a5f; border-radius: 8px;
             margin: .5rem .75rem .75rem; padding: 1rem; }}
  .tr-header {{ display: flex; align-items: center; gap: .75rem; margin-bottom: .6rem;
                font-size: 11px; font-weight: 600; color: #60a5fa; }}
  .tr-meta {{ margin-left: auto; color: #475569; font-weight: 400; }}
  .lang-badge {{ background: #164e63; color: #67e8f9; border-radius: 4px;
                 padding: 1px 6px; font-size: 10px; font-weight: 700; }}
  .tr-text {{ font-size: 12px; line-height: 1.65; color: #cbd5e1;
              white-space: pre-wrap; font-family: "Segoe UI", Roboto, sans-serif; }}

  /* ---- Colour helpers ---- */
  .green  {{ color: #4ade80 !important; }}
  .red    {{ color: #f87171 !important; }}
  .blue   {{ color: #60a5fa !important; }}
  .purple {{ color: #c084fc !important; }}
  .amber  {{ color: #fbbf24 !important; }}
</style>
</head>
<body>

<div class="top-bar">
  <h1>&#128222; <span>Cityflo</span> — Customer Call Audit</h1>
  <div class="top-meta">
    <strong>{esc(period)}</strong><br>
    Generated {esc(generated)} IST
  </div>
</div>

{summary_html}

<div class="filter-bar">
  <label>Search agent:</label>
  <input type="text" id="agentFilter" placeholder="Type agent name..." oninput="filterAgents()">
  <button class="btn-toggle" onclick="expandAll()">Expand All</button>
  <button class="btn-toggle" onclick="collapseAll()">Collapse All</button>
</div>

<div class="agents" id="agentsContainer">
  {no_data}
  {''.join(agent_blocks)}
</div>

<script>
  function toggleAgent(id) {{
    document.getElementById('body-' + id).classList.toggle('open');
    document.getElementById('chev-' + id).classList.toggle('open');
  }}
  function expandAll() {{
    document.querySelectorAll('.agent-body').forEach(b => b.classList.add('open'));
    document.querySelectorAll('.chevron').forEach(c => c.classList.add('open'));
  }}
  function collapseAll() {{
    document.querySelectorAll('.agent-body').forEach(b => b.classList.remove('open'));
    document.querySelectorAll('.chevron').forEach(c => c.classList.remove('open'));
  }}
  function filterAgents() {{
    const q = document.getElementById('agentFilter').value.toLowerCase();
    document.querySelectorAll('.agent-card').forEach(card => {{
      card.style.display = card.querySelector('.agent-name').textContent.toLowerCase().includes(q) ? '' : 'none';
    }});
  }}
  function toggleTr(id) {{
    document.getElementById(id).classList.toggle('open');
  }}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Cityflo Call Audit Report")
    parser.add_argument("--days",              type=int,  default=DEFAULT_DAYS,
                        help=f"Past N days to audit (default: {DEFAULT_DAYS})")
    parser.add_argument("--output",            type=str,  default=None,
                        help="Save report to file (.txt or .html)")
    parser.add_argument("--transcribe",        action="store_true",
                        help="Transcribe voice recordings using Whisper")
    parser.add_argument("--transcribe-limit",  type=int,  default=DEFAULT_LIMIT,
                        help=f"Max new recordings to transcribe (default: {DEFAULT_LIMIT})")
    parser.add_argument("--whisper-model",     type=str,  default=DEFAULT_MODEL,
                        choices=["tiny", "base", "small", "medium", "large"],
                        help=f"Whisper model size (default: {DEFAULT_MODEL})")
    parser.add_argument("--max-duration",      type=int,  default=None,
                        help="Skip recordings longer than N seconds (e.g. 300 = skip calls over 5 min)")
    args = parser.parse_args()

    if args.days < 1:
        print("Error: --days must be >= 1", file=sys.stderr)
        sys.exit(1)

    env    = load_env(ENV_FILE)
    db_url = env.get("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not found in env file", file=sys.stderr)
        sys.exit(1)

    print("Connecting to database...", end=" ", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        print("OK")
    except Exception as e:
        print(f"\nConnection failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching calls for the past {args.days} day(s)...", end=" ", flush=True)
    try:
        rows = fetch_calls(conn, args.days)
        print(f"{len(rows)} records found")
    except Exception as e:
        print(f"\nQuery failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

    # Transcription (optional)
    transcripts: dict = {}
    if args.transcribe:
        rec_count = sum(1 for r in rows if r["voice_recording_url"])
        print(f"\nTranscribing recordings ({rec_count} in result set, limit={args.transcribe_limit})...")

        exotel_sid   = env.get("EXOTEL_SID", "").strip()
        exotel_key   = env.get("EXOTEL_API_KEY", "").strip()
        exotel_token = env.get("EXOTEL_API_TOKEN", "").strip()
        # Use API Key + Token if available, fall back to SID + Key
        if exotel_key and exotel_token:
            auth = (exotel_key, exotel_token)
        elif exotel_sid and exotel_key:
            auth = (exotel_sid, exotel_key)
        else:
            auth = None
            print("  Warning: EXOTEL_SID/EXOTEL_API_KEY not set in env — recording downloads may fail with 401.")

        transcripts = transcribe_recordings(rows, args.transcribe_limit, args.whisper_model, auth=auth, max_duration=args.max_duration)

    # Build report
    is_html = args.output and args.output.lower().endswith(".html")
    report  = (
        build_html_report(rows, args.days, transcripts)
        if is_html
        else build_report(rows, args.days)
    )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nReport saved to: {args.output}")
    else:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        print()
        print(report)


if __name__ == "__main__":
    main()
