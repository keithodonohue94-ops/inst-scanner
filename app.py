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

from scanner import scan_tickers, UNIVERSES as _BASE_UNIVERSES
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
    if request.path in ("/api/health",):
        return None
    if request.path.startswith("/api/"):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        if not hmac.compare_digest(auth[7:], _VALID_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
    return None

# ── In-memory cache ───────────────────────────────────────────────────────────
# Structure:  CACHE[(universe_key, days_int)] = {
#   "results":    [...],
#   "scanned_at": "2026-07-28T02:00:00Z",
#   "count":      42,
# }
CACHE: dict = {}
SCANNING: set = set()
_cache_lock = threading.Lock()

DEFAULT_DAYS = 270
BACKFILL_DAYS = 270         # 270 days covers three full quarters of 13F filings
SCAN_WINDOWS  = [270]       # days lookbacks to pre-cache

SKIP_SCHEDULED = set()   # no universes excluded from scheduled scans


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _cache_key(universe: str, days: int) -> tuple:
    return (universe, days)


def _run_scan(universe_key: str, days: int):
    """Fetch EDGAR filings for one universe/window, update cache, persist to DB."""
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
        entry = {
            "results":    results,
            "scanned_at": now,
            "count":      len(results),
            "universe":   universe_key,
            "days":       days,
        }
        with _cache_lock:
            CACHE[key] = entry
        # Persist to PostgreSQL so results survive redeploys
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
        "cache":     len(CACHE),
    })


@app.route("/api/stats")
def stats():
    with _cache_lock:
        cached = [
            {
                "universe":   k[0],
                "days":       k[1],
                "count":      v["count"],
                "scanned_at": v["scanned_at"],
            }
            for k, v in CACHE.items()
        ]
    last_scan = (
        max(cached, key=lambda x: x["scanned_at"]) if cached else None
    )
    return jsonify({
        "status":    "ok",
        "last_scan": last_scan,
        "cached":    cached,
        "scanning":  [{"universe": k[0], "days": k[1]} for k in SCANNING],
        "universes": list(UNIVERSES.keys()),
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

    with _cache_lock:
        data = CACHE.get(key)

    # Cache miss — check DB (scan may have completed on a different worker)
    if not data:
        data = db.load_scan(universe, days)
        if data:
            with _cache_lock:
                CACHE[key] = data

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
