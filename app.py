"""
inst-scanner/app.py
Flask backend for the Poppa Alpha Institutional tab.

Endpoints:
  GET  /api/health                                   — health check
  GET  /api/stats                                    — cache status
  GET  /api/results?universe=portfolio&days=90       — cached filings
  POST /api/scan  {"universe":"portfolio","days":90} — trigger async re-scan
  GET  /api/poll?universe=portfolio&days=90          — poll scan progress
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

from scanner import scan_tickers, UNIVERSES

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
SCAN_ORDER   = ["portfolio", "myportfolio", "smh", "soxx", "ndx100", "sp500"]
SCAN_WINDOWS = [30, 90]          # days lookbacks to pre-cache


# ── Scanner logic ─────────────────────────────────────────────────────────────

def _cache_key(universe: str, days: int) -> tuple:
    return (universe, days)


def _run_scan(universe_key: str, days: int):
    """Fetch EDGAR filings for one universe/window and update cache."""
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
        with _cache_lock:
            CACHE[key] = {
                "results":    results,
                "scanned_at": now,
                "count":      len(results),
                "universe":   universe_key,
                "days":       days,
            }
        logger.info("Done: %s/%dd — %d filings", universe_key, days, len(results))
    except Exception as exc:
        logger.error("Scan error (%s/%dd): %s", universe_key, days, exc)
    finally:
        SCANNING.discard(key)


def _background_scheduler():
    """
    On startup: scan key universes for each lookback window.
    Repeat every 6 hours so filings stay current.
    """
    while True:
        for ukey in SCAN_ORDER:
            for days in SCAN_WINDOWS:
                _run_scan(ukey, days)
                time.sleep(5)
        logger.info("Institutional scan cycle complete. Sleeping 6 hours.")
        time.sleep(6 * 3_600)


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
        # Auto-trigger a scan if we have nothing yet
        if universe in UNIVERSES and key not in SCANNING:
            t = threading.Thread(
                target=_run_scan, args=(universe, days), daemon=True
            )
            t.start()
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bg = threading.Thread(target=_background_scheduler, daemon=True)
    bg.start()

    import os
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)
