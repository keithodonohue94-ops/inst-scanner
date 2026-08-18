"""
inst-scanner/app.py
Flask backend for the Osprey Institutional tab.

Endpoints:
  GET  /api/health                                   — health check
  GET  /api/stats                                    — cache status
  GET  /api/results?universe=portfolio&days=90       — cached filings
  POST /api/scan  {"universe":"portfolio","days":90} — trigger async re-scan
  GET  /api/poll?universe=portfolio&days=90          — poll scan progress
  POST /api/backfill                                 — 90-day backfill all universes
"""

import threading
import time
import logging
import hmac
import hashlib
import os
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

from scanner import scan_tickers, backfill_prior_holdings, UNIVERSES as _BASE_UNIVERSES
import db

# Mutable universe registry — starts from scanner.py defaults, then
# custom universes loaded from DB (and synced from the frontend) are merged in.
UNIVERSES: dict = dict(_BASE_UNIVERSES)

# ── Setup ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── Auth ──────────────────────────────────────────────────────────────────────
_OSPREY_SECRET   = os.environ.get("OSPREY_SECRET",   "osprey-secret-change-me")
_OSPREY_PASSWORD = os.environ.get("OSPREY_PASSWORD",  "changeme")

def _make_token(password: str) -> str:
    return hmac.new(_OSPREY_SECRET.encode(), password.encode(), hashlib.sha256).hexdigest()

_VALID_TOKEN = _make_token(_OSPREY_PASSWORD)

@app.before_request
def check_auth():
    if request.method == "OPTIONS":
        return None
    if request.path in ("/api/health", "/api/db-check", "/api/reset-baseline", "/api/verify-ciks", "/api/test-institution", "/api/stats"):
        return None
    if request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        if not hmac.compare_digest(auth[7:], _VALID_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
    return None

# ── Scan state ────────────────────────────────────────────────────────────────
SCANNING: set = set()          # keys currently being scanned; prevents duplicates
_PRIOR_BACKFILL_RUNNING = False

DEFAULT_DAYS = 270
BACKFILL_DAYS = 270         # 270 days covers three full quarters of 13F filings
SCAN_WINDOWS  = [270]       # days lookbacks to pre-cache

SKIP_SCHEDULED = set()   # no universes excluded from scheduled scans


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _cache_key(universe: str, days: int) -> tuple:
    return (universe, days)


def _run_scan(universe_key: str, days: int):
    """Fetch EDGAR filings for one universe/window and persist to DB."""
    key = _cache_key(universe_key, days)
    if key in SCANNING:
        logger.info("Already scanning %s/%dd — skipping", universe_key, days)
        return
    tickers = UNIVERSES.get(universe_key)
    if not tickers:
        logger.warning("Unknown universe: %s", universe_key)
        return

    SCANNING.add(key)
    logger.info(
        "Starting institutional scan: %s (%d tickers, %dd lookback)",
        universe_key, len(tickers), days,
    )
    try:
        results = scan_tickers(tickers, days=days, enrich=True)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        db.save_scan(universe_key, days, now, results)
        logger.info("Done: %s/%dd — %d filings", universe_key, days, len(results))
    except Exception as exc:
        logger.error("Scan error (%s/%dd): %s", universe_key, days, exc)
    finally:
        SCANNING.discard(key)


def _daily_fetch():
    """
    Daily scheduled fetch — runs at 7am EST (12:00 UTC).
    Scans all universes except those in SKIP_SCHEDULED (sp500, ndx100 —
    too many tickers for a daily EDGAR scan).
    """
    to_scan = [k for k in UNIVERSES if k not in SKIP_SCHEDULED]
    logger.info("=== Daily institutional fetch starting (%dd, %d universes) ===",
                BACKFILL_DAYS, len(to_scan))
    for ukey in to_scan:
        _run_scan(ukey, BACKFILL_DAYS)
        time.sleep(5)   # be polite to SEC EDGAR
    logger.info("=== Daily institutional fetch complete ===")


def _run_backfill():
    """Backfill across all scannable universes (excludes SKIP_SCHEDULED)."""
    to_scan = [k for k in UNIVERSES if k not in SKIP_SCHEDULED]
    logger.info("=== Institutional backfill starting (%d universes, %dd) ===",
                len(to_scan), BACKFILL_DAYS)
    for ukey in to_scan:
        _run_scan(ukey, BACKFILL_DAYS)
        time.sleep(5)
    logger.info("=== Institutional backfill complete ===")


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    db_ok = False
    db_rows = 0
    try:
        import psycopg2
        conn = psycopg2.connect(db.DATABASE_URL) if db.DATABASE_URL else None
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM inst_scan_cache")
            db_rows = cur.fetchone()[0]
            cur.close()
            conn.close()
            db_ok = True
    except Exception as e:
        logger.warning("Health DB check failed: %s", e)
    return jsonify({
        "status":    "ok",
        "timestamp": _now(),
        "db":        {"connected": db_ok, "cached_rows": db_rows},
    })


@app.route("/api/stats")
def stats():
    scan_stats = db.load_scan_stats()
    last_scan = scan_stats[0] if scan_stats else None   # already sorted DESC by scanned_at
    return jsonify({
        "status":                 "ok",
        "last_scan":              last_scan,
        "cached":                 scan_stats,
        "scanning":               [{"universe": k[0], "days": k[1]} for k in SCANNING],
        "universes":              list(UNIVERSES.keys()),
        "prior_backfill_running": _PRIOR_BACKFILL_RUNNING,
    })


@app.route("/api/results")
def results():
    universe = request.args.get("universe", "portfolio")
    days     = int(request.args.get("days", DEFAULT_DAYS))

    # Always read from DB — source of truth. No in-memory cache dependency.
    data = db.load_scan(universe, days)

    if not data:
        return jsonify({
            "status":     "no_data",
            "results":    [],
            "count":      0,
            "scanned_at": None,
            "universe":   universe,
            "days":       days,
        })

    return jsonify({
        "status":     "ok",
        "results":    data["results"],
        "count":      data["count"],
        "scanned_at": data["scanned_at"],
        "universe":   universe,
        "days":       days,
    })


@app.route("/api/scan", methods=["POST"])
def trigger_scan():
    body     = request.get_json(silent=True) or {}
    universe = body.get("universe", "portfolio")
    days     = int(body.get("days", DEFAULT_DAYS))
    key      = _cache_key(universe, days)

    if universe not in UNIVERSES:
        return jsonify({"error": f"Unknown universe: {universe}"}), 400

    if key in SCANNING:
        return jsonify({"status": "already_scanning", "universe": universe, "days": days})

    t = threading.Thread(target=_run_scan, args=(universe, days), daemon=True)
    t.start()
    return jsonify({"status": "scanning", "universe": universe, "days": days})


@app.route("/api/poll")
def poll():
    universe = request.args.get("universe", "portfolio")
    days     = int(request.args.get("days", DEFAULT_DAYS))
    key      = _cache_key(universe, days)

    data     = db.load_scan(universe, days)
    scanning = key in SCANNING
    if data:
        return jsonify({
            "ready":      True,
            "scanning":   scanning,
            "results":    data["results"],
            "count":      data["count"],
            "scanned_at": data["scanned_at"],
        })
    return jsonify({"ready": False, "scanning": scanning})


@app.route("/api/universes/sync", methods=["POST"])
def sync_universes():
    """
    Receive universe definitions from the frontend and persist them to DB.
    Body: [{"key": "aiinfra", "name": "AI Infrastructure", "tickers": ["NVDA", ...]}]
    Merges into the live UNIVERSES dict so the daily fetch picks them up.
    """
    universes = request.get_json(silent=True)
    if not isinstance(universes, list):
        return jsonify({"error": "Expected a JSON array of universe objects"}), 400

    valid = []
    for u in universes:
        key     = u.get("key", "").strip()
        name    = u.get("name", "").strip()
        tickers = u.get("tickers", [])
        if not key or not name or not isinstance(tickers, list) or not tickers:
            continue
        valid.append({"key": key, "name": name, "tickers": tickers})

    if not valid:
        return jsonify({"error": "No valid universe objects found"}), 400

    # Full replace of custom universes in live UNIVERSES dict:
    # 1. Remove any keys that were previously synced but aren't in this payload
    incoming_keys = {u["key"] for u in valid}
    stale = [k for k in list(UNIVERSES.keys()) if k not in incoming_keys and k not in _BASE_UNIVERSES]
    for k in stale:
        UNIVERSES.pop(k, None)
    # 2. Upsert the incoming set
    for u in valid:
        UNIVERSES[u["key"]] = u["tickers"]

    # Persist to DB (full replace — deletes stale rows too)
    db.save_custom_universes(valid)

    logger.info("Synced %d universes from frontend. Total UNIVERSES: %d", len(valid), len(UNIVERSES))
    return jsonify({
        "status":   "ok",
        "synced":   len(valid),
        "universes": list(UNIVERSES.keys()),
    })


@app.route("/api/verify-ciks")
def verify_ciks():
    """
    Check every hardcoded institution CIK against EDGAR and return
    the actual entity name. Use this to catch wrong CIK mappings.
    """
    from scanner import TOP_INSTITUTIONS, _sec_get, SUBMISSIONS_BASE, _padded_cik
    results = []
    for display_name, cik in TOP_INSTITUTIONS:
        url = f"{SUBMISSIONS_BASE}/CIK{_padded_cik(cik)}.json"
        resp = _sec_get(url, timeout=10)
        actual_name = None
        if resp and resp.ok:
            try:
                actual_name = resp.json().get("name")
            except Exception:
                pass
        results.append({
            "our_label":   display_name,
            "cik":         cik,
            "actual_name": actual_name,
            "match":       display_name.split()[0].upper() in (actual_name or "").upper(),
        })
    return jsonify(results)


@app.route("/api/test-institution")
def test_institution():
    """
    Diagnostic endpoint — test a single institution end-to-end.
    Usage: /api/test-institution?name=VANGUARD+GROUP&days=270
    Returns raw EFTS hits, filing info, first 5 XML lines, and any matched tickers.
    """
    from scanner import _find_institution_13f, _get_info_table_xml, _parse_holdings, TICKER_NAME_MAP
    name = request.args.get("name", "VANGUARD GROUP")
    days = int(request.args.get("days", 270))
    sample_tickers = {"NVDA", "AAPL", "MSFT", "AMZN", "META", "AMD", "AVGO", "TSLA"}

    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    filing = _find_institution_13f(name, start_date)
    if not filing:
        return jsonify({"institution": name, "filing": None, "error": "No 13F found via EFTS"})

    xml = _get_info_table_xml(filing["cik"], filing["accession"])
    xml_preview = xml[:500] if xml else None
    xml_len = len(xml) if xml else 0

    holdings = _parse_holdings(xml, sample_tickers) if xml else []

    return jsonify({
        "institution": name,
        "filing":      filing,
        "xml_found":   xml is not None,
        "xml_bytes":   xml_len,
        "xml_preview": xml_preview,
        "matched_holdings": holdings,
    })


@app.route("/api/db-check")
def db_check():
    """
    Diagnostic: inspect inst_holdings baseline table.
    Optional ?ticker=CRDO to filter by ticker.
    Returns period distribution + sample rows.
    """
    ticker = request.args.get("ticker", "").upper() or None
    if not db._USE_PG:
        return jsonify({"error": "No DB connected"})
    try:
        import psycopg2.extras
        conn = db._conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Period distribution
        cur.execute("SELECT period, COUNT(*) as cnt FROM inst_holdings GROUP BY period ORDER BY period")
        periods = [{"period": r["period"], "count": r["cnt"]} for r in cur.fetchall()]

        # Total rows
        cur.execute("SELECT COUNT(*) FROM inst_holdings")
        total = cur.fetchone()[0]

        # Sample rows (filtered by ticker if provided)
        if ticker:
            cur.execute("""
                SELECT institution_cik, ticker, shares, value_k, period, filed_date, updated_at
                FROM inst_holdings WHERE ticker = %s ORDER BY institution_cik LIMIT 50
            """, (ticker,))
        else:
            cur.execute("""
                SELECT institution_cik, ticker, shares, value_k, period, filed_date, updated_at
                FROM inst_holdings ORDER BY updated_at DESC LIMIT 50
            """)
        rows = [dict(r) for r in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify({
            "total_rows":        total,
            "period_distribution": periods,
            "sample_rows":       rows,
            "filter_ticker":     ticker,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/reset-baseline", methods=["POST"])
def reset_baseline():
    """
    Delete all inst_holdings rows that are NOT from the backfill quarter.
    Pass ?keep_period=Q4+2025 (or whatever the backfill quarter is).
    This cleans up rows incorrectly written by a scan run.
    """
    keep = request.args.get("keep_period", "").strip()
    if not keep:
        return jsonify({"error": "Must supply ?keep_period=Q4+2025"}), 400
    if not db._USE_PG:
        return jsonify({"error": "No DB"}), 500
    try:
        import psycopg2
        conn = db._conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM inst_holdings WHERE period != %s", (keep,))
        to_delete = cur.fetchone()[0]
        cur.execute("DELETE FROM inst_holdings WHERE period != %s", (keep,))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM inst_holdings")
        remaining = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({"deleted": to_delete, "remaining": remaining, "kept_period": keep})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/backfill", methods=["POST"])
def trigger_backfill():
    """
    Trigger a 90-day backfill across all universes in background.
    Use this once after deploy to populate the DB.
    """
    t = threading.Thread(target=_run_backfill, daemon=True)
    t.start()
    return jsonify({
        "status":    "started",
        "message":   f"90-day backfill running for all universes: {list(UNIVERSES.keys())}",
        "universes": list(UNIVERSES.keys()),
        "days":      BACKFILL_DAYS,
    })


@app.route("/api/scan-all", methods=["POST"])
def trigger_scan_all():
    """
    Trigger a fresh scan across all universes in background.
    Results are persisted to DB as each universe completes.
    """
    t = threading.Thread(target=_daily_fetch, daemon=True)
    t.start()
    return jsonify({
        "status":    "started",
        "universes": list(UNIVERSES.keys()),
        "message":   f"Scanning {len(UNIVERSES)} universes in background",
    })


@app.route("/api/backfill-prior", methods=["POST"])
def trigger_backfill_prior():
    """
    Seed inst_holdings with the second-most-recent 13F per institution across
    all universes. This establishes the prior-quarter baseline so the next scan
    can derive Initiated / Added / Reduced / Exited signals correctly.

    Uses seed_prior_holdings (unconditional upsert) — always resets the baseline
    regardless of what is currently stored.
    """
    # Collect all unique tickers across every universe
    all_tickers = list({t for tickers in UNIVERSES.values() for t in tickers})

    def _run():
        global _PRIOR_BACKFILL_RUNNING
        _PRIOR_BACKFILL_RUNNING = True
        logger.info("=== Prior-quarter backfill starting (%d unique tickers) ===",
                    len(all_tickers))
        try:
            # Always clear the entire baseline first so we never layer on top
            # of stale data from a previous quarter or a bad scan write.
            db.clear_holdings_baseline()
            logger.info("=== Baseline cleared — seeding prior quarter ===")
            result = backfill_prior_holdings(all_tickers, days=BACKFILL_DAYS)
            logger.info("=== Prior-quarter backfill complete: %s ===", result)
        finally:
            _PRIOR_BACKFILL_RUNNING = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({
        "status":       "started",
        "message":      "Backfilling prior-quarter holdings for all universes",
        "total_tickers": len(all_tickers),
        "universes":    list(UNIVERSES.keys()),
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Startup ───────────────────────────────────────────────────────────────────

def _startup():
    # 1. Initialise DB schema
    db.init_db()

    # 2. Load persisted custom universe definitions and merge into UNIVERSES
    custom = db.load_custom_universes()
    for u in custom:
        UNIVERSES[u["key"]] = u["tickers"]
    if custom:
        logger.info("Loaded %d custom universes from DB. Total UNIVERSES: %d", len(custom), len(UNIVERSES))

    # 3. Check if DB has any data; if not, kick off initial backfill
    cached = db.load_all_cached()
    if not cached:
        logger.info("No cached data in DB — triggering initial backfill")
        t = threading.Thread(target=_run_backfill, daemon=True)
        t.start()
    else:
        logger.info("DB has %d cached scans — results served directly from DB on request", len(cached))

    # 3. Daily scheduler at 12:00 UTC = 7:00 AM EST
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _daily_fetch,
        "cron",
        hour=12, minute=0,
        id="daily_inst_fetch",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — daily institutional fetch at 12:00 UTC (7:00 AM EST)")


# Run startup logic once (gunicorn spawns multiple workers; only master needs this)
# Using a simple flag to avoid double-init in dev mode with reloader
_started = False

@app.before_request
def _lazy_startup():
    global _started
    if not _started:
        _started = True
        _startup()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _startup()
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)
