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
    """Create inst_scan_cache, inst_custom_universes, and inst_holdings tables if they don't exist."""
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inst_holdings (
                institution_cik INTEGER NOT NULL,
                ticker          TEXT    NOT NULL,
                shares          BIGINT,
                value_k         BIGINT,
                period          TEXT,
                filed_date      TEXT,
                updated_at      TEXT,
                PRIMARY KEY (institution_cik, ticker)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("inst_scan_cache + inst_custom_universes + inst_holdings tables ready")
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


def load_scan(universe: str, days: int) -> dict | None:
    """Load a single universe/days entry from DB. Returns None if not found."""
    if not _USE_PG:
        return None
    try:
        conn = _conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            "SELECT universe, days, scanned_at, results, count FROM inst_scan_cache WHERE universe=%s AND days=%s",
            (universe, days)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return {
            "results":    json.loads(row["results"]),
            "scanned_at": row["scanned_at"],
            "count":      row["count"],
            "universe":   row["universe"],
            "days":       row["days"],
        }
    except Exception as e:
        logger.error("load_scan failed (%s/%dd): %s", universe, days, e)
        return None


def get_prior_holdings(institution_cik: int) -> dict:
    """
    Load last known holdings for an institution.
    Returns dict: ticker -> {shares, value_k, period, filed_date}
    """
    if not _USE_PG:
        return {}
    try:
        conn = _conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            "SELECT ticker, shares, value_k, period, filed_date FROM inst_holdings WHERE institution_cik = %s",
            (institution_cik,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {
            r["ticker"]: {
                "shares":     r["shares"],
                "value_k":    r["value_k"],
                "period":     r["period"],
                "filed_date": r["filed_date"],
            }
            for r in rows
        }
    except Exception as e:
        logger.error("get_prior_holdings failed (CIK %d): %s", institution_cik, e)
        return {}


def upsert_holdings(institution_cik: int, holdings: list, updated_at: str):
    """
    Upsert current holdings snapshot for an institution.
    holdings = [{ticker, shares, value_k, period, filed_date}]
    Only updates if the period is newer than what's stored.
    """
    if not _USE_PG:
        return
    try:
        conn = _conn()
        cur = conn.cursor()
        for h in holdings:
            cur.execute("""
                INSERT INTO inst_holdings
                    (institution_cik, ticker, shares, value_k, period, filed_date, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (institution_cik, ticker) DO UPDATE SET
                    shares     = EXCLUDED.shares,
                    value_k    = EXCLUDED.value_k,
                    period     = EXCLUDED.period,
                    filed_date = EXCLUDED.filed_date,
                    updated_at = EXCLUDED.updated_at
                WHERE inst_holdings.period IS NULL
                   OR EXCLUDED.filed_date > inst_holdings.filed_date
            """, (
                institution_cik,
                h["ticker"],
                h.get("shares"),
                h.get("value_k"),
                h.get("period"),
                h.get("filed_date"),
                updated_at,
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Upserted %d holdings for CIK %d", len(holdings), institution_cik)
    except Exception as e:
        logger.error("upsert_holdings failed (CIK %d): %s", institution_cik, e)


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
