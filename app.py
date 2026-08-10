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
from datetime import datetime, timezone

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

DEFAULT_DAYS = 90
BACKFILL_DAYS = 90          # lookback for scheduled + backfill runs
SCAN_WINDOWS  = [90]        # days lookbacks to pre-cache

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
    return jsonify({"status": "ok", "timestamp": _now()})


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
    key      = _cache_key(universe, days)

    with _cache_lock:
        data = CACHE.get(key)

    if not data:
        # No cached data — return no_data without auto-triggering a scan.
        # Data is populated by the daily scheduled fetch or explicit Run Scan.
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

    # 3. Load any previously saved results from PostgreSQL into memory
    #    so results are available immediately after a redeploy
    cached = db.load_all_cached()
    if cached:
        with _cache_lock:
            CACHE.update(cached)
        logger.info("Cache pre-loaded from DB (%d entries)", len(cached))
    else:
        # No DB data yet — kick off a full backfill in background
        logger.info("No cached data in DB — triggering initial backfill")
        t = threading.Thread(target=_run_backfill, daemon=True)
        t.start()

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
