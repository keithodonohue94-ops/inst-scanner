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
    """Create inst_scan_cache table if it doesn't exist."""
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
        conn.commit()
        cur.close()
        conn.close()
        logger.info("inst_scan_cache table ready")
    except Exception as e:
        logger.error("init_db failed: %s", e)


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
