"""
inst-scanner/db.py
PostgreSQL persistence for institutional scan results.
Falls back silently when DATABASE_URL is not set.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
_USE_PG = False

if DATABASE_URL:
    try:
        import psycopg2
        import psycopg2.extras
        _USE_PG = True
        logger.info("inst-scanner: PostgreSQL persistence enabled")
    except ImportError:
        logger.warning("psycopg2 not available — results won't be persisted across deploys")


def _conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create inst_scan_cache and inst_custom_universes tables if they don't exist."""
    if not _USE_PG:
        return
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inst_scan_cache (
                id         SERIAL PRIMARY KEY,
                universe   TEXT    NOT NULL,
                days       INTEGER NOT NULL,
                scanned_at TEXT    NOT NULL,
                results    TEXT    NOT NULL,
                count      INTEGER NOT NULL DEFAULT 0,
                UNIQUE(universe, days)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inst_custom_universes (
                key      TEXT PRIMARY KEY,
                name     TEXT NOT NULL,
                tickers  TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("inst_scan_cache + inst_custom_universes tables ready")
    except Exception as e:
        logger.error("init_db failed: %s", e)


def save_custom_universes(universes: list):
    """
    Full replace of custom universe definitions.
    Universes NOT in the incoming list are deleted; the rest are upserted.
    universes = [{key, name, tickers}]
    """
    if not _USE_PG:
        return
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        incoming_keys = [u["key"] for u in universes]
        conn = _conn()
        cur = conn.cursor()
        # Delete universes no longer in the frontend list
        if incoming_keys:
            cur.execute(
                "DELETE FROM inst_custom_universes WHERE key != ALL(%s)",
                (incoming_keys,)
            )
        else:
            cur.execute("DELETE FROM inst_custom_universes")
        # Upsert the current set
        for u in universes:
            cur.execute("""
                INSERT INTO inst_custom_universes (key, name, tickers, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    name       = EXCLUDED.name,
                    tickers    = EXCLUDED.tickers,
                    updated_at = EXCLUDED.updated_at
            """, (u["key"], u["name"], json.dumps(u["tickers"]), now))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Universe sync: %d universes active in DB", len(universes))
    except Exception as e:
        logger.error("save_custom_universes failed: %s", e)


def load_custom_universes() -> list:
    """Load custom universe definitions from DB. Returns [{key, name, tickers}]"""
    if not _USE_PG:
        return []
    try:
        conn = _conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT key, name, tickers FROM inst_custom_universes ORDER BY key")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        result = [{"key": r["key"], "name": r["name"], "tickers": json.loads(r["tickers"])} for r in rows]
        logger.info("Loaded %d custom universes from DB", len(result))
        return result
    except Exception as e:
        logger.error("load_custom_universes failed: %s", e)
        return []


def save_scan(universe: str, days: int, scanned_at: str, results: list):
    """Upsert scan results for a universe/days combination."""
    if not _USE_PG:
        return
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inst_scan_cache (universe, days, scanned_at, results, count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (universe, days) DO UPDATE SET
                scanned_at = EXCLUDED.scanned_at,
                results    = EXCLUDED.results,
                count      = EXCLUDED.count
        """, (universe, days, scanned_at, json.dumps(results), len(results)))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Saved %d filings to DB (%s / %dd)", len(results), universe, days)
    except Exception as e:
        logger.error("save_scan failed: %s", e)


def load_all_cached() -> dict:
    """
    Load all persisted scan results from DB.
    Returns dict keyed by (universe, days) matching the in-memory CACHE format.
    """
    if not _USE_PG:
        return {}
    try:
        conn = _conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT universe, days, scanned_at, results, count FROM inst_scan_cache")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        out = {}
        for row in rows:
            key = (row["universe"], row["days"])
            out[key] = {
                "results":    json.loads(row["results"]),
                "scanned_at": row["scanned_at"],
                "count":      row["count"],
                "universe":   row["universe"],
                "days":       row["days"],
            }
        logger.info("Loaded %d cached inst scans from PostgreSQL", len(out))
        return out
    except Exception as e:
        logger.error("load_all_cached failed: %s", e)
        return {}
