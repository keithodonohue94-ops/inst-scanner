"""
inst-scanner/holdings.py
Full-portfolio 13F persistence and quarter-over-quarter analysis.

Why this exists
  The original scan kept only the rows matching a universe and compared them with a
  single, manually seeded "prior quarter" baseline. If that baseline was missing or
  seeded for different tickers, every holding showed as INITIATED. Tickers were matched
  by company-name fragments for ~130 hardcoded names, so most of the S&P 500 never
  matched at all.

What this does
  * Stores every tracked manager's FULL 13F portfolio for each quarter
    (inst_filings: one row per 13F; inst_positions: every equity position by CUSIP).
  * Keeps the manager's total portfolio value, position count and top-10 concentration,
    so position weights and manager tiers (passive / diversified / concentrated) can be
    computed.
  * Maps tickers to CUSIPs (FMP profile) so holdings are matched exactly.
  * Changes (Initiated / Added / Reduced / Exited) are derived by comparing each
    manager's latest stored quarter with the quarter before it — no manual baseline.
  * Only the tickers in our universes are returned by the API (S&P 500, Nasdaq 100,
    SOXX, SMH, AI infrastructure and custom universes), but everything is stored.
"""

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_STABLE = "https://financialmodelingprep.com/stable"
SQLITE_PATH = os.getenv("INST_SQLITE_PATH", "/tmp/inst_holdings.db")   # local/dev only

_USE_PG = False
if DATABASE_URL:
    try:
        import psycopg2
        import psycopg2.extras
        _USE_PG = True
    except ImportError:
        pass

# Index / passive managers: their buying mostly reflects fund flows, not views.
_PASSIVE_NAMES = re.compile(r"vanguard|blackrock|state street|geode|northern trust|"
                            r"charles schwab|schwab charles|invesco|dimensional", re.I)
# Systematic / multi-strategy books: thousands of model-driven, short-horizon positions —
# active, but a single position rarely reflects a fundamental view.
_QUANT_NAMES = re.compile(r"renaissance|two sigma|d\.?\s?e\.? shaw|aqr|citadel|millennium|"
                          r"jane street|susquehanna|hudson river|virtu|point72|balyasny|"
                          r"qube|cubist|man group|winton|de shaw", re.I)

QUARTERS_TO_KEEP = 3          # latest + two prior quarters per manager


# ── tiny DB layer (Postgres in production, SQLite for local tests) ─────────────
class _DB:
    def __init__(self):
        if _USE_PG:
            self.c = psycopg2.connect(DATABASE_URL)
        else:
            self.c = sqlite3.connect(SQLITE_PATH)

    def q(self, sql, params=()):
        cur = self.c.cursor()
        cur.execute(sql if _USE_PG else sql.replace("%s", "?"), params)
        return cur

    def many(self, sql, rows):
        if not rows:
            return
        cur = self.c.cursor()
        if _USE_PG:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        else:
            n = len(rows[0])
            cur.executemany(sql.replace("VALUES %s", "VALUES (" + ",".join("?" * n) + ")"), rows)

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        if et is None:
            self.c.commit()
        else:
            self.c.rollback()
        self.c.close()
        return False


def init_tables():
    with _DB() as d:
        d.q("""CREATE TABLE IF NOT EXISTS inst_filings (
                   accession TEXT PRIMARY KEY, cik INTEGER NOT NULL, filer TEXT, form TEXT,
                   period_end TEXT, period TEXT, filed_date TEXT,
                   total_value REAL, n_positions INTEGER, top10_pct REAL, tier TEXT,
                   ingested_at TEXT)""")
        d.q("""CREATE TABLE IF NOT EXISTS inst_positions (
                   accession TEXT NOT NULL, cik INTEGER NOT NULL, period_end TEXT,
                   cusip TEXT NOT NULL, cusip8 TEXT, issuer TEXT, title TEXT,
                   shares REAL, value REAL,
                   PRIMARY KEY (accession, cusip))""")
        d.q("CREATE INDEX IF NOT EXISTS idx_instpos_cusip8 ON inst_positions(cusip8)")
        d.q("CREATE INDEX IF NOT EXISTS idx_instpos_cik_period ON inst_positions(cik, period_end)")
        d.q("""CREATE TABLE IF NOT EXISTS ticker_cusip (
                   ticker TEXT PRIMARY KEY, cusip TEXT, cusip8 TEXT, name TEXT, updated_at TEXT)""")


# ── tracked tickers (what the API returns) ────────────────────────────────────
def _fmp(endpoint: str, params: dict):
    if not FMP_API_KEY:
        return None
    try:
        r = requests.get(f"{FMP_STABLE}/{endpoint}", params={**params, "apikey": FMP_API_KEY}, timeout=20)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        logger.warning("FMP %s: %s", endpoint, e)
        return None


def universe_tickers(base_universes: dict) -> dict:
    """{universe_key: [tickers]} — shared DB universes + scanner defaults + live S&P 500 /
    Nasdaq 100 constituents from FMP."""
    out = {k: list(v) for k, v in base_universes.items()}
    try:
        with _DB() as d:
            for key, tickers in d.q("SELECT key, tickers FROM universes").fetchall():
                out[key] = json.loads(tickers or "[]")
    except Exception as e:
        logger.debug("universes table: %s", e)
    for key, ep in (("sp500", "sp500-constituent"), ("ndx100", "nasdaq-constituent")):
        if not out.get(key):
            data = _fmp(ep, {})
            if isinstance(data, list) and data:
                out[key] = [r.get("symbol") for r in data if r.get("symbol")]
    return {k: sorted({t.upper() for t in v if t}) for k, v in out.items()}


def refresh_cusips(tickers: list[str], max_age_days: int = 90) -> int:
    """Fill ticker → CUSIP from FMP profiles for tickers we don't have (or are stale)."""
    if not FMP_API_KEY:
        logger.warning("FMP_API_KEY not set — CUSIP mapping unavailable")
        return 0
    with _DB() as d:
        have = {t: u for t, u in d.q("SELECT ticker, updated_at FROM ticker_cusip").fetchall()}
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_days * 86400
    todo = [t for t in tickers if t not in have or
            datetime.fromisoformat(have[t]).timestamp() < cutoff]
    n = 0
    for t in todo:
        prof = _fmp("profile", {"symbol": t})
        p = prof[0] if isinstance(prof, list) and prof else {}
        cusip = (p.get("cusip") or "").strip().upper()
        with _DB() as d:
            d.q("""INSERT INTO ticker_cusip (ticker, cusip, cusip8, name, updated_at) VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (ticker) DO UPDATE SET cusip = EXCLUDED.cusip, cusip8 = EXCLUDED.cusip8,
                       name = EXCLUDED.name, updated_at = EXCLUDED.updated_at""",
                (t, cusip or None, cusip[:8] or None, p.get("companyName"),
                 datetime.now(timezone.utc).isoformat()))
        n += 1
        time.sleep(0.12)
    return n


# ── 13F parsing ───────────────────────────────────────────────────────────────
_TAG = lambda name: re.compile(rf"<(?:\w+:)?{name}(?:\s[^>]*)?>\s*(.*?)\s*</(?:\w+:)?{name}>", re.I | re.S)
_RE_BLOCK, _RE_CUSIP, _RE_ISSUER = _TAG("infoTable"), _TAG("cusip"), _TAG("nameOfIssuer")
_RE_TITLE, _RE_VALUE, _RE_SHARES = _TAG("titleOfClass"), _TAG("value"), _TAG("sshPrnamt")
_RE_SHTYPE, _RE_PUTCALL = _TAG("sshPrnamtType"), _TAG("putCall")


def parse_info_table(xml: str) -> list[dict]:
    """Every long equity position (shares, not bonds/options), aggregated by CUSIP.
    13F values are reported in dollars (since 2023)."""
    agg: dict[str, dict] = {}
    for block in _RE_BLOCK.findall(xml or ""):
        pc = _RE_PUTCALL.search(block)
        if pc and pc.group(1).strip():
            continue                                    # options
        st = _RE_SHTYPE.search(block)
        if st and st.group(1).strip().upper() == "PRN":
            continue                                    # bonds / principal amounts
        cm = _RE_CUSIP.search(block)
        if not cm:
            continue
        cusip = cm.group(1).strip().upper()
        try:
            value = float(_RE_VALUE.search(block).group(1).replace(",", ""))
        except Exception:
            value = 0.0
        try:
            shares = float(_RE_SHARES.search(block).group(1).replace(",", ""))
        except Exception:
            shares = 0.0
        im, tm = _RE_ISSUER.search(block), _RE_TITLE.search(block)
        a = agg.setdefault(cusip, {"cusip": cusip, "issuer": im.group(1).strip() if im else "",
                                   "title": tm.group(1).strip() if tm else "", "shares": 0.0, "value": 0.0})
        a["shares"] += shares
        a["value"] += value
    return list(agg.values())


def _tier(filer: str, n_positions: int, top10_pct: float | None) -> str:
    """passive = index houses (flows into index funds/ETFs); quant = systematic/multi-strat;
    concentrated = high-conviction book; diversified = everything else (incl. large active
    houses like T. Rowe, Capital, Wellington that hold thousands of names across many funds)."""
    if _PASSIVE_NAMES.search(filer or ""):
        return "passive"
    if _QUANT_NAMES.search(filer or ""):
        return "quant"
    if (top10_pct or 0) >= 50 or (n_positions or 0) <= 150:
        return "concentrated"
    return "diversified"


def fetch_info_table(cik: int, accession: str, sec_get) -> tuple[str | None, str]:
    """Raw 13F information-table XML for a filing, plus a reason when it can't be found.
    The filing index lists both SEC's rendered web view (under an xslForm13F… folder) and
    the raw XML; only the raw file contains <infoTable> elements."""
    acc_nodash = accession.replace("-", "")
    idx = sec_get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{accession}-index.htm",
                  extra_headers={"Accept": "text/html"}, timeout=15)
    if not idx or not idx.ok:
        return None, "index not found"
    links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx.text, re.I)
    raw = [l for l in links if "/xsl" not in l.lower()]
    if not raw:
        return None, "no raw xml in index"
    # The information table is the raw XML that isn't the cover page (primary_doc.xml);
    # prefer names that say so, otherwise take the largest-looking candidate list order.
    cands = [l for l in raw if not l.lower().endswith("primary_doc.xml")] or raw
    cands.sort(key=lambda l: 0 if re.search(r"info|table|holding", l, re.I) else 1)
    for link in cands:
        r = sec_get("https://www.sec.gov" + link, extra_headers={"Accept": "application/xml,text/xml"}, timeout=90)
        if r is not None and r.ok and re.search(r"<(?:\w+:)?infoTable[\s>]", r.text, re.I):
            return r.text, ""
    return None, "no infoTable in raw xml"


def _all_13f(data: dict, sec_get) -> list[tuple]:
    """(form, filed, accession, period) for 13F-HR filings, newest first — from the recent
    list and, when that holds too few (heavy 13G filers), the older history pages."""
    def rows(block):
        return list(zip(block.get("form", []), block.get("filingDate", []),
                        block.get("accessionNumber", []), block.get("reportDate", [])))
    out = [r for r in rows((data.get("filings") or {}).get("recent") or {}) if r[0] == "13F-HR"]
    if len(out) < QUARTERS_TO_KEEP + 1:
        for f in ((data.get("filings") or {}).get("files") or [])[:3]:
            r = sec_get(f"https://data.sec.gov/submissions/{f.get('name')}", timeout=20)
            if r is not None and r.ok:
                out += [x for x in rows(r.json()) if x[0] == "13F-HR"]
            if len(out) >= QUARTERS_TO_KEEP + 1:
                break
    return sorted(out, key=lambda x: x[1], reverse=True)


def ingest_filer(display_name: str, cik: int, sec_get, info_table_xml, submissions_base: str,
                 quarters: int = QUARTERS_TO_KEEP) -> dict:
    """Store the latest `quarters` 13F-HR portfolios for one manager (skips ones already stored)."""
    stats = {"filer": display_name, "new_filings": 0, "positions": 0}
    resp = sec_get(f"{submissions_base}/CIK{str(cik).zfill(10)}.json", timeout=15)
    if not resp or not resp.ok:
        stats["error"] = "submissions unavailable"
        return stats
    data = resp.json()
    filer = data.get("name") or display_name
    recent = (data.get("filings") or {}).get("recent") or {}
    picked: dict[str, dict] = {}                       # period_end → newest original 13F-HR
    stats["skipped"] = []
    for form, date, acc, period in _all_13f(data, sec_get):
        if not period:
            continue                                    # amendments (13F-HR/A) excluded: often partial
        if period not in picked:
            picked[period] = {"accession": acc, "filed_date": date, "period_end": period}
        if len(picked) >= quarters:
            break
    with _DB() as d:
        stored = {r[0] for r in d.q("SELECT accession FROM inst_filings WHERE cik = %s", (cik,)).fetchall()}
    for f in picked.values():
        if f["accession"] in stored:
            continue
        xml, why = fetch_info_table(cik, f["accession"], sec_get)
        if not xml:
            stats["skipped"].append(f'{f["period_end"]} {f["accession"]}: {why}')
            continue
        pos = parse_info_table(xml)
        if not pos:
            stats["skipped"].append(f'{f["period_end"]} {f["accession"]}: no equity positions parsed')
            continue
        total = sum(p["value"] for p in pos)
        top10 = sum(sorted((p["value"] for p in pos), reverse=True)[:10])
        top10_pct = round(top10 / total * 100, 1) if total else None
        y, m = int(f["period_end"][:4]), int(f["period_end"][5:7])
        with _DB() as d:
            d.q("""INSERT INTO inst_filings (accession, cik, filer, form, period_end, period, filed_date,
                       total_value, n_positions, top10_pct, tier, ingested_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (accession) DO NOTHING""",
                (f["accession"], cik, filer, "13F-HR", f["period_end"], f"Q{(m - 1) // 3 + 1} {y}",
                 f["filed_date"], total, len(pos), top10_pct, _tier(filer, len(pos), top10_pct),
                 datetime.now(timezone.utc).isoformat()))
            d.many("""INSERT INTO inst_positions (accession, cik, period_end, cusip, cusip8, issuer, title, shares, value)
                      VALUES %s ON CONFLICT (accession, cusip) DO NOTHING""",
                   [(f["accession"], cik, f["period_end"], p["cusip"], p["cusip"][:8], p["issuer"][:120],
                     p["title"][:60], p["shares"], p["value"]) for p in pos])
        stats["new_filings"] += 1
        stats["positions"] += len(pos)
    # keep only the latest N quarters per manager
    with _DB() as d:
        old = [r[0] for r in d.q("""SELECT accession FROM inst_filings WHERE cik = %s
                                    ORDER BY period_end DESC""", (cik,)).fetchall()][quarters:]
        for acc in old:
            d.q("DELETE FROM inst_positions WHERE accession = %s", (acc,))
            d.q("DELETE FROM inst_filings WHERE accession = %s", (acc,))
    return stats


# ── query: latest quarter vs prior quarter, universe tickers only ─────────────
def available_periods() -> list[dict]:
    """Stored quarters, newest first, with how many managers have filed each."""
    with _DB() as d:
        rows = d.q("""SELECT period, MAX(period_end), COUNT(DISTINCT cik) FROM inst_filings
                       GROUP BY period ORDER BY MAX(period_end) DESC""").fetchall()
        total = d.q("SELECT COUNT(DISTINCT cik) FROM inst_filings").fetchone()[0] or 0
    return [{"period": r[0], "period_end": r[1], "managers": r[2], "total_managers": total} for r in rows]


def default_period(periods: list[dict]) -> str | None:
    """Newest quarter that at least half the tracked managers have filed (13Fs land up to
    45 days after quarter end, so the newest quarter is often only partly in)."""
    for p in periods:
        if p["total_managers"] and p["managers"] * 2 >= p["total_managers"]:
            return p["period"]
    return periods[0]["period"] if periods else None


def holdings_view(tickers: set[str], period: str | None = None) -> list[dict]:
    """One row per (manager, ticker) in the manager's filing for `period` (or its latest stored
    quarter when no period is given), with the change against that manager's previous stored
    quarter. Exits included. Managers with no filing for `period` are left out."""
    with _DB() as d:
        tc = [r for r in d.q("SELECT ticker, cusip8, name FROM ticker_cusip WHERE cusip8 IS NOT NULL").fetchall()
              if r[0] in tickers]
        tmap = {c8: t for t, c8, _ in tc}
        names = {t: (n or "") for t, _, n in tc}
        filings = d.q("""SELECT accession, cik, filer, period_end, period, filed_date, total_value,
                                n_positions, top10_pct, tier FROM inst_filings ORDER BY cik, period_end DESC""").fetchall()
        by_cik: dict[int, list] = {}
        for f in filings:
            by_cik.setdefault(f[1], []).append(f)
        if not tmap:
            return []
        marks = ",".join(["%s"] * len(tmap))
        pos = d.q(f"""SELECT accession, cusip8, shares, value FROM inst_positions
                      WHERE cusip8 IN ({marks})""", tuple(tmap.keys())).fetchall()
    holding: dict[tuple, dict] = {}                     # (accession, ticker) → {shares, value}
    for acc, c8, sh, val in pos:
        h = holding.setdefault((acc, tmap[c8]), {"shares": 0.0, "value": 0.0})
        h["shares"] += sh or 0
        h["value"] += val or 0
    out = []
    for cik, fl in by_cik.items():
        if period:
            idx = next((i for i, f in enumerate(fl) if f[4] == period), None)
            if idx is None:
                continue                                # this manager hasn't filed that quarter
            fl = fl[idx:]
        cur, prev = fl[0], (fl[1] if len(fl) > 1 else None)
        cur_tickers = {t for (a, t) in holding if a == cur[0]}
        prev_tickers = {t for (a, t) in holding if prev and a == prev[0]}
        for t in sorted(cur_tickers | prev_tickers):
            c = holding.get((cur[0], t))
            p = holding.get((prev[0], t)) if prev else None
            if prev is None:
                change = "New filer"                    # no earlier quarter stored yet
            elif c and not p:
                change = "Initiated"
            elif p and not c:
                change = "Exited"
            elif c["shares"] > p["shares"]:
                change = "Added"
            elif c["shares"] < p["shares"]:
                change = "Reduced"
            else:
                change = "Unchanged"
            sh, val = (c or {}).get("shares", 0.0), (c or {}).get("value", 0.0)
            psh, pval = (p or {}).get("shares"), (p or {}).get("value")
            w = round(val / cur[6] * 100, 3) if cur[6] else None
            pw = round(pval / prev[6] * 100, 3) if (prev and pval is not None and prev[6]) else None
            out.append({
                "ticker": t, "company": names.get(t, ""), "filer": cur[2], "cik": cik, "form": "13F-HR",
                "period": cur[4], "period_end": cur[3], "filed_date": cur[5], "accession": cur[0],
                "shares": sh, "value": val, "value_k": val,          # value_k kept for the existing UI (dollars)
                "prev_period": prev[4] if prev else None,
                "prev_shares": psh, "prev_value": pval,
                "shares_delta": (sh - psh) if psh is not None else None,
                "change": change,
                "weight_pct": w, "prev_weight_pct": pw,
                "weight_delta": round(w - pw, 3) if (w is not None and pw is not None) else None,
                "filer_total_value": cur[6], "filer_positions": cur[7], "filer_top10_pct": cur[8],
                "filer_tier": _tier(cur[2], cur[7] or 0, cur[8]),
            })
    return out


def store_status() -> dict:
    with _DB() as d:
        f = d.q("SELECT COUNT(*), COUNT(DISTINCT cik), MAX(period_end), MAX(filed_date) FROM inst_filings").fetchone()
        p = d.q("SELECT COUNT(*) FROM inst_positions").fetchone()
        c = d.q("SELECT COUNT(*), COUNT(cusip8) FROM ticker_cusip").fetchone()
        per = d.q("SELECT period, COUNT(*) FROM inst_filings GROUP BY period ORDER BY period").fetchall()
    return {"filings": f[0], "managers": f[1], "latest_period_end": f[2], "latest_filed": f[3],
            "positions": p[0], "tickers_mapped": c[1], "tickers_checked": c[0],
            "filings_by_period": {r[0]: r[1] for r in per}}
