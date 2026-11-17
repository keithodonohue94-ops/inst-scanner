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
_PASSIVE_NAMES = re.compile(r"vanguard|blackrock|state street|ssga|geode|northern trust|"
                            r"charles schwab|schwab charles|invesco|dimensional|"
                            r"legal\s*&\s*general|parametric|nuveen|teachers insurance|tiaa|"
                            r"bny mellon|bank of new york|mellon|amundi|norges bank|"
                            r"principal financial|ameriprise|lpl financial", re.I)
# Banks, broker-dealers and wealth platforms: 13Fs aggregate client accounts, market-making and
# hedges, so a position change rarely reflects an investment view. Excluded from "active".
_BANK_NAMES = re.compile(r"goldman sachs|morgan stanley|jp\s?morgan|j\.\s?p\.\s?morgan|bank of america|"
                         r"merrill lynch|citigroup|wells fargo|ubs group|ubs ag|ubs securities|barclays|"
                         r"deutsche bank|royal bank of canada|rbc capital|bnp paribas|hsbc|credit suisse|"
                         r"nomura|mizuho|mufg|mitsubishi ufj|sumitomo mitsui|raymond james|stifel|"
                         r"jefferies|td securities|toronto[- ]dominion|bank of montreal|bmo|scotia|"
                         r"cibc|societe generale|natixis|banco santander|interactive brokers|"
                         r"oppenheimer & co|piper sandler|cowen|commonwealth equity|osaic|cetera|edward jones", re.I)
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
        d.q("""CREATE TABLE IF NOT EXISTS ticker_splits (
                   ticker TEXT NOT NULL, split_date TEXT NOT NULL, numerator REAL, denominator REAL,
                   PRIMARY KEY (ticker, split_date))""")
        d.q("""CREATE TABLE IF NOT EXISTS ticker_splits_checked (ticker TEXT PRIMARY KEY, checked_at TEXT)""")


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


def refresh_splits(tickers: list[str], max_age_days: int = 7) -> int:
    """Stock-split history from FMP for every tracked ticker, re-checked weekly. A split between two quarter-ends
    multiplies every holder's share count (APH 2:1, CRWD 4:1), which read as heavy buying."""
    if not FMP_API_KEY:
        return 0
    with _DB() as d:
        have = {t: u for t, u in d.q("SELECT ticker, checked_at FROM ticker_splits_checked").fetchall()}
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_days * 86400
    todo = [t for t in tickers if t not in have or datetime.fromisoformat(have[t]).timestamp() < cutoff]
    n = 0
    for t in todo:
        data = _fmp("splits", {"symbol": t})
        rows = []
        for x in data if isinstance(data, list) else []:
            try:
                num, den = float(x.get("numerator") or 0), float(x.get("denominator") or 0)
            except (TypeError, ValueError):
                continue
            dt = str(x.get("date") or "")[:10]
            if num > 0 and den > 0 and dt and num != den:
                rows.append((t, dt, num, den))
        with _DB() as d:
            for r in rows:
                d.q("""INSERT INTO ticker_splits (ticker, split_date, numerator, denominator) VALUES (%s,%s,%s,%s)
                       ON CONFLICT (ticker, split_date) DO UPDATE SET numerator = EXCLUDED.numerator,
                           denominator = EXCLUDED.denominator""", r)
            if data is not None:                        # an FMP failure is retried next run
                d.q("""INSERT INTO ticker_splits_checked (ticker, checked_at) VALUES (%s,%s)
                       ON CONFLICT (ticker) DO UPDATE SET checked_at = EXCLUDED.checked_at""",
                    (t, datetime.now(timezone.utc).isoformat()))
        n += 1
        time.sleep(0.12)
    return n


def split_factor(splits: list, after: str, upto: str) -> float:
    """Shares multiplier for splits dated in (after, upto] — e.g. 2.0 for a 2-for-1, 0.1 for a 1-for-10."""
    f = 1.0
    for dt, num, den in splits:
        if after < dt <= upto:
            f *= num / den
    return f


_STATE_SUFFIX = re.compile(r"\b(TX|DE|NJ|NY|NV|MD|PA|OH|VA|MA|CA|IL|MN|WI|IN|MO|GA|FL|NC|WA|OR|CO|AZ|UT)$")
_NOT_COMMON = re.compile(r"PFD|PREF|DEP\s*SH|DEPOSITARY|NOTE|DEB|BOND|WARR|WT\b|RIGHT|\bRT\b|UNIT|CONV|SR\b|%", re.I)


def _norm_issuer(s: str) -> str:
    """'EXXON MOBIL CORP', 'EXXONMOBIL HOLDINGS CORP' and 'Exxon Mobil Corporation /TX/' → 'EXXONMOBIL'."""
    s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
    s = re.sub(r"\b(INC|CORP|CORPORATION|CO|COMPANY|LTD|PLC|NEW|HLDGS|HOLDINGS|THE|COM|CL A|CLASS A|N V|NV)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # spaces dropped last: "EXXON MOBIL CORP" and "EXXONMOBIL HOLDINGS CORP" (its 2026 holding-company CUSIP) match
    return _STATE_SUFFIX.sub("", s).strip().replace(" ", "")


def cusip_aliases(mapped: dict, occurrences: dict, issuer_of: dict, period_of_acc: dict,
                  title_of: dict | None = None, debug: dict | None = None) -> dict:
    """Old CUSIP → ticker, for companies whose CUSIP changed (XOM, OKE: every holder "initiated" in Q3 because Q2
    was filed under the old CUSIP). A candidate CUSIP is linked only when (a) its issuer name matches exactly one
    mapped ticker's, (b) it never appears in the same filing as that ticker's current CUSIP — share classes and
    preferreds do co-occur — and (c) every filing of it is older than the first filing of the current CUSIP.
    mapped: cusip8 → ticker; occurrences: cusip8 → set of accessions; issuer_of: cusip8 → issuer name."""
    by_name: dict[str, set] = {}
    for c8, t in mapped.items():
        n = _norm_issuer(issuer_of.get(c8, ""))
        if n:
            by_name.setdefault(n, set()).add(c8)
    out = {}
    title_of = title_of or {}
    for c8, accs in occurrences.items():
        if c8 in mapped:
            continue
        cur = by_name.get(_norm_issuer(issuer_of.get(c8, "")))
        if not cur or len({mapped[m] for m in cur}) != 1:
            continue
        m = next(iter(cur))
        m_accs = occurrences.get(m, set())
        why = None
        if _NOT_COMMON.search(title_of.get(c8) or ""):
            why = "not common stock: " + (title_of.get(c8) or "")
        elif not m_accs:
            why = "current CUSIP has no filings"
        else:
            both = len(accs & m_accs)
            # a manager mid-transition can list both CUSIPs once; share classes / preferreds co-occur routinely
            if both > max(1, 0.05 * len(accs | m_accs)):
                why = f"appears with the current CUSIP in {both} filings"
            else:
                new_first = min(period_of_acc.get(a, "9") for a in m_accs)
                later = sum(1 for a in accs if period_of_acc.get(a, "") >= new_first)
                if later > 0.1 * len(accs):
                    why = f"{later} of {len(accs)} filings are not older than the current CUSIP's first ({new_first})"
        if debug is not None:
            debug[c8] = {"issuer": issuer_of.get(c8), "title": title_of.get(c8), "ticker": mapped[m],
                         "filings": len(accs), "linked": why is None, "why_not": why}
        if why is None:
            out[c8] = mapped[m]
    return out


def flag_corporate_actions(rows: list, min_holders: int = 5) -> dict:
    """Tickers where most holders' share counts moved by the same ratio with no split on record — a stock merger
    or spin-off (EQR, PSKY). Not buying or selling, and not undoable here, so these rows are flagged and the flow
    views leave them out. Returns ticker → note."""
    by: dict[str, list] = {}
    for r in rows:
        if r.get("prev_shares") and r.get("shares") and r["prev_shares"] > 0 and r["shares"] > 0:
            by.setdefault(r["ticker"], []).append(r["shares"] / r["prev_shares"])
    out = {}
    for t, rs in by.items():
        if len(rs) < min_holders:
            continue
        m = sorted(rs)[len(rs) // 2]
        if 0.8 <= m <= 1.25:
            continue
        near = sum(1 for x in rs if abs(x / m - 1) <= 0.10)
        # Either most holders moved by the same ratio, or the median holder's count moved by 40%+ either way —
        # large managers' positions don't move that much in aggregate from trading (EQR: median x2.09, but the
        # holders also traded, so only 7 of 16 sat within 10% of it).
        if near >= 0.7 * len(rs) or (len(rs) >= 8 and not 0.7 <= m <= 1.4):
            out[t] = f"median holder's share count changed x{m:.2f} ({near} of {len(rs)} within 10%) with no split on record (merger or spin-off?)"
    return out


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
    """passive = index houses (flows into index funds/ETFs); bank = banks/brokers/wealth platforms
    (client + market-making books); quant = systematic/multi-strat;
    concentrated = high-conviction book; diversified = everything else (incl. large active
    houses like T. Rowe, Capital, Wellington that hold thousands of names across many funds)."""
    if _PASSIVE_NAMES.search(filer or ""):
        return "passive"
    if _BANK_NAMES.search(filer or ""):
        return "bank"
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


def value_scale(holding: dict, period_of: dict) -> dict:
    """Per-filing multiplier for 13F values. Since 2023 the information table reports dollars, but some filings
    (T. Rowe Price's, for one) still come in thousands: AMD then shows $0.61 a share against $612 in every other
    filing. A filing whose implied prices sit ~1,000x below (or above) the other managers' for the same tickers
    and quarter gets x1000 (or /1000). Needs at least 3 comparable tickers; otherwise the filing is left alone."""
    px: dict[tuple, list] = {}                          # (period, ticker) → implied prices across filings
    for (acc, t), h in holding.items():
        if h["shares"] > 0 and h["value"] > 0:
            px.setdefault((period_of.get(acc), t), []).append(h["value"] / h["shares"])
    med = {k: sorted(v)[len(v) // 2] for k, v in px.items() if len(v) >= 3}
    ratios: dict[str, list] = {}
    for (acc, t), h in holding.items():
        m = med.get((period_of.get(acc), t))
        if m and h["shares"] > 0 and h["value"] > 0:
            ratios.setdefault(acc, []).append(h["value"] / h["shares"] / m)
    out = {}
    for acc, rs in ratios.items():
        if len(rs) < 3:
            continue
        r = sorted(rs)[len(rs) // 2]
        if 0.0002 <= r <= 0.005:
            out[acc] = 1000
        elif 200 <= r <= 5000:
            out[acc] = 0.001
    return out


ALIAS_DEBUG: dict = {}          # last holdings_view's CUSIP-alias decisions (for /api/holdings/aliases)


def issuer_cusips(pattern: str) -> list[dict]:
    """Every stored CUSIP whose issuer name starts with `pattern` — to see why a CUSIP change was or wasn't linked."""
    with _DB() as d:
        rows = d.q("""SELECT cusip8, MAX(issuer), MAX(title), COUNT(*), MIN(period_end), MAX(period_end)
                      FROM inst_positions WHERE UPPER(issuer) LIKE %s GROUP BY cusip8 ORDER BY 6 DESC""",
                   (pattern.upper() + "%",)).fetchall()
        mapped = {c8: t for t, c8 in d.q("SELECT ticker, cusip8 FROM ticker_cusip").fetchall()}
    return [{"cusip8": r[0], "issuer": r[1], "title": r[2], "positions": r[3], "first": r[4], "last": r[5],
             "ticker": mapped.get(r[0]), "norm": _norm_issuer(r[1])} for r in rows]


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
        all_mapped = {r[0] for r in d.q("SELECT cusip8 FROM ticker_cusip WHERE cusip8 IS NOT NULL").fetchall()}
        marks = ",".join(["%s"] * len(tmap))
        issuer_of = {c8: iss for c8, iss in d.q(f"""SELECT cusip8, MAX(issuer) FROM inst_positions
                                                     WHERE cusip8 IN ({marks}) GROUP BY cusip8""", tuple(tmap.keys())).fetchall()}
        # candidate old CUSIPs: same normalised issuer name, not mapped to any ticker
        iss_names = {_norm_issuer(n) for n in issuer_of.values() if n}
        cand, cand_title = {}, {}
        if iss_names:
            for c8, iss, ttl in d.q("""SELECT cusip8, MAX(issuer), MAX(title) FROM inst_positions WHERE cusip8 IS NOT NULL
                                       GROUP BY cusip8""").fetchall():
                if c8 not in all_mapped and _norm_issuer(iss) in iss_names:
                    cand[c8], cand_title[c8] = iss, ttl
        keys = tuple(tmap.keys()) + tuple(cand.keys())
        marks = ",".join(["%s"] * len(keys))
        pos = d.q(f"""SELECT accession, cusip8, shares, value FROM inst_positions
                      WHERE cusip8 IN ({marks})""", keys).fetchall()
        split_rows = d.q("SELECT ticker, split_date, numerator, denominator FROM ticker_splits").fetchall()
    splits: dict[str, list] = {}
    for t, dt, num, den in split_rows:
        if t in tickers and num and den:
            splits.setdefault(t, []).append((dt, num, den))
    if cand:
        occ: dict[str, set] = {}
        for acc, c8, _sh, _v in pos:
            occ.setdefault(c8, set()).add(acc)
        ALIAS_DEBUG.clear()
        alias = cusip_aliases(tmap, occ, {**issuer_of, **cand}, {f[0]: f[3] for f in filings}, cand_title, ALIAS_DEBUG)
        if alias:
            logger.info("13F CUSIP aliases: %s", alias)
        tmap = {**tmap, **alias}
        pos = [p for p in pos if p[1] in tmap]
    holding: dict[tuple, dict] = {}                     # (accession, ticker) → {shares, value}
    for acc, c8, sh, val in pos:
        h = holding.setdefault((acc, tmap[c8]), {"shares": 0.0, "value": 0.0})
        h["shares"] += sh or 0
        h["value"] += val or 0
    scale = value_scale(holding, {f[0]: f[4] for f in filings})
    if any(k != 1 for k in scale.values()):
        for (acc, _t), h in holding.items():
            h["value"] *= scale.get(acc, 1)
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
            sf = split_factor(splits.get(t, []), prev[3] or "", cur[3] or "") if prev else 1.0
            if p and sf != 1.0:
                p = {**p, "shares": round(p["shares"] * sf)}          # prior holding restated in post-split shares
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
            ctot = cur[6] * scale.get(cur[0], 1) if cur[6] else cur[6]
            ptot = prev[6] * scale.get(prev[0], 1) if (prev and prev[6]) else None
            w = round(val / ctot * 100, 3) if ctot else None
            pw = round(pval / ptot * 100, 3) if (pval is not None and ptot) else None
            out.append({
                "ticker": t, "company": names.get(t, ""), "filer": cur[2], "cik": cik, "form": "13F-HR",
                "period": cur[4], "period_end": cur[3], "filed_date": cur[5], "accession": cur[0],
                "shares": sh, "value": val, "value_k": val,          # value_k kept for the existing UI (dollars)
                "prev_period": prev[4] if prev else None,
                "prev_shares": psh, "prev_value": pval,
                "shares_delta": (sh - psh) if psh is not None else None,
                "split_factor": sf if sf != 1.0 else None,
                "change": change,
                "weight_pct": w, "prev_weight_pct": pw,
                "weight_delta": round(w - pw, 3) if (w is not None and pw is not None) else None,
                "filer_total_value": ctot, "filer_positions": cur[7], "filer_top10_pct": cur[8],
                "filer_tier": _tier(cur[2], cur[7] or 0, cur[8]),
            })
    flags = flag_corporate_actions(out)
    for r in out:
        if r["ticker"] in flags:
            r["corp_action"] = flags[r["ticker"]]
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
