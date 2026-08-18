"""
inst-scanner/scanner.py
SEC EDGAR 13F-HR institutional holdings scanner.

Approach: hardcoded CIKs for top ~25 institutional investors.
For each institution:
  1. Fetch their filing history via data.sec.gov/submissions/CIK{cik}.json
  2. Find the most recent 13F-HR within the lookback window
  3. Download the information table XML from the filing directory
  4. Parse holdings and match against our universe tickers

This mirrors exactly how the insider scanner fetches Form 4 — submissions API,
no EFTS full-text search (which does not index 13F holdings data).
"""

import os
import re
import time
import logging
import requests
from datetime import datetime, timedelta, timezone

import db

logger = logging.getLogger(__name__)

# ── Universe definitions (fallback — overridden by DB-synced universes) ───────
UNIVERSES = {
    "portfolio": [
        "ALAB","MRVL","CRDO","IREN","APLD","MU","GFS","FLNC",
        "AMD","MOD","TER","ARM","ANET","VICR","ORA","QCOM","STRL",
    ],
    "myportfolio": [
        "ALAB","MRVL","CRDO","MU","MOD","STRL","ANET","VICR",
        "ORA","FLNC","ETN","PWR","NVDA","SMCI","INTC","MPWR","VRT",
    ],
    "soxx": [
        "AMD","NVDA","MU","AVGO","INTC","AMAT","TSM","KLAC","LRCX","TXN",
        "MRVL","ADI","MPWR","NXPI","TER","QCOM","ASML","ALAB","MCHP","CRDO",
        "ON","ASX","ENTG","MTSI","UMC","SWKS","WOLF","ACLS","CRUS","STX","FORM",
    ],
    "smh": [
        "NVDA","TSM","AVGO","AMD","ASML","TXN","MU","ADI","AMAT","QCOM",
        "KLAC","LRCX","INTC","MRVL","CDNS","SNPS","MPWR","TER","NXPI","STM",
        "ARM","MCHP","ALAB","ON","SWKS","WOLF",
    ],
    "aiinfra": [
        "ETN","PWR","LITE","MOD","STRL","ANET","CLS","MRVL","MPWR","CRDO",
        "VRT","NVDA","SMCI","ALAB","MU","AMD","QCOM","ARM","TSM","AVGO",
        "LRCX","AMAT","KLAC","ADI","TXN","SNPS","CDNS","IREN","APLD","GFS",
    ],
    "cybersec": [
        "S","QLYS","OKTA","CRWD","PANW","ZS","NET","FTNT","CYBR","TENB",
    ],
    "energy": [
        "ENPH","VICR","ORA","FLNC","CEG","VST","ETR","FSLR","NEE",
        "WULF","NRG","RUN","PLUG","NNE","OKLO","CCJ","SMR",
    ],
    "orbital": [
        "BKSY","PLTR","PL","RDW","RKLB","ASTS","LUNR","SPCE",
    ],
    "rareearths": [
        "MP","IDR","USAR","TMC","UUUU","DNN",
    ],
}

# ── SEC EDGAR config ──────────────────────────────────────────────────────────

_SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "OspreyResearch/1.0 research@osprey.com")
HEADERS = {
    "User-Agent": _SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

EDGAR_BASE       = "https://www.sec.gov"
SUBMISSIONS_BASE = "https://data.sec.gov/submissions"

_SEC_BASE_DELAY = 0.5   # 500ms between requests

def _sec_get(url: str, params: dict = None, extra_headers: dict = None,
             timeout: int = 20, retries: int = 4) -> requests.Response | None:
    """GET a SEC EDGAR URL with exponential backoff on 429."""
    hdrs = {**HEADERS, **(extra_headers or {})}
    backoff = 30
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
        except Exception as exc:
            logger.debug("SEC GET error %s: %s", url[-60:], exc)
            time.sleep(_SEC_BASE_DELAY)
            return None

        if resp.status_code == 429:
            wait = backoff * (2 ** attempt)
            logger.warning("429 rate-limited — waiting %ds (attempt %d/%d)", wait, attempt + 1, retries)
            time.sleep(wait)
            continue

        time.sleep(_SEC_BASE_DELAY)
        return resp

    logger.error("Gave up on %s after %d retries (all 429)", url[-80:], retries)
    return None


# ── Top institutional investors with known EDGAR CIKs ────────────────────────
# CIKs verified against SEC EDGAR company search.
# Each entry: (display_name, integer_cik)
TOP_INSTITUTIONS = [
    ("Vanguard Group",            102909),
    ("BlackRock",                 1364742),
    ("State Street",              93751),
    ("Fidelity (FMR LLC)",        315066),
    ("T. Rowe Price",             80255),
    ("JPMorgan Chase",            19617),
    ("Goldman Sachs",             886982),
    ("Morgan Stanley",            895421),
    ("Invesco",                   914208),
    ("Northern Trust",            73124),
    ("Wellington Management",     107263),
    ("Geode Capital Management",  1444822),
    ("Capital Research Global",   315966),
    ("Charles Schwab",            316206),
    ("Citadel Advisors",          1423298),
    ("Millennium Management",     1273931),
    ("Renaissance Technologies",  1037389),
    ("Duquesne Family Office",     1536411),   # Druckenmiller — CIK confirmed
    ("Two Sigma Investments",     1447362),   # Two Sigma Investments LP
    ("D.E. Shaw",                 1009207),
    ("AQR Capital Management",   1280790),
    ("Coatue Management",         1336528),
    ("Tiger Global Management",   1167483),
    ("Viking Global Investors",   1103804),
    ("Balyasny Asset Management", 1283699),
    ("Point72 Asset Management",  1603466),
]

# Ticker → fragment of company name as it appears in 13F nameOfIssuer field.
TICKER_NAME_MAP = {
    "NVDA": "NVIDIA",              "AMD":   "ADVANCED MICRO DEVICES",
    "INTC": "INTEL CORP",          "AVGO":  "BROADCOM",
    "MU":   "MICRON TECHNOLOGY",   "AMAT":  "APPLIED MATERIALS",
    "TSM":  "TAIWAN SEMICONDUCTOR","KLAC":  "KLA CORP",
    "LRCX": "LAM RESEARCH",        "TXN":   "TEXAS INSTRUMENTS",
    "MRVL": "MARVELL",             "ADI":   "ANALOG DEVICES",
    "MPWR": "MONOLITHIC POWER",    "NXPI":  "NXP SEMICONDUCT",
    "TER":  "TERADYNE",            "QCOM":  "QUALCOMM",
    "ASML": "ASML",                "ALAB":  "ASTERA LABS",
    "MCHP": "MICROCHIP TECH",      "CRDO":  "CREDO TECH",
    "ON":   "ON SEMICONDUCTOR",    "ENTG":  "ENTEGRIS",
    "CDNS": "CADENCE",             "SNPS":  "SYNOPSYS",
    "ARM":  "ARM HOLDINGS",        "ANET":  "ARISTA NETWORKS",
    "SMCI": "SUPER MICRO",         "VRT":   "VERTIV",
    "ETN":  "EATON CORP",          "PWR":   "QUANTA SERVICES",
    "PLTR": "PALANTIR",            "CRWD":  "CROWDSTRIKE",
    "PANW": "PALO ALTO",           "MSFT":  "MICROSOFT",
    "AAPL": "APPLE INC",           "AMZN":  "AMAZON",
    "GOOGL":"ALPHABET",            "GOOG":  "ALPHABET",
    "META": "META PLATFORMS",      "TSLA":  "TESLA",
    "IREN": "IRIS ENERGY",         "MOD":   "MODINE",
    "STRL": "STERLING INFRA",      "FLNC":  "FLUENCE ENERGY",
    "GFS":  "GLOBALFOUNDRIES",     "APLD":  "APPLIED DIGITAL",
    "VICR": "VICOR CORP",          "ORA":   "ORMAT",
    "LITE": "LUMENTUM",            "FSLR":  "FIRST SOLAR",
    "NEE":  "NEXTERA ENERGY",      "CEG":   "CONSTELLATION ENERGY",
    "VST":  "VISTRA",              "NRG":   "NRG ENERGY",
    "PLUG": "PLUG POWER",          "RUN":   "SUNRUN",
    "MP":   "MP MATERIALS",        "RKLB":  "ROCKET LAB",
    "ASTS": "AST SPACEMOBILE",     "LUNR":  "INTUITIVE MACHINES",
    "BKSY": "BLACKSKY",            "PL":    "PLANET LABS",
    "SPCE": "VIRGIN GALACTIC",     "RDW":   "REDWIRE",
    "S":    "SENTINELONE",         "OKTA":  "OKTA",
    "ZS":   "ZSCALER",             "NET":   "CLOUDFLARE",
    "FTNT": "FORTINET",            "CYBR":  "CYBERARK",
    "TENB": "TENABLE",             "QLYS":  "QUALYS",
    "WMT":  "WALMART",             "COST":  "COSTCO",
    "NFLX": "NETFLIX",             "AMGN":  "AMGEN",
    "SHOP": "SHOPIFY",             "TMUS":  "T-MOBILE",
    "PEP":  "PEPSICO",             "GILD":  "GILEAD",
    "BKNG": "BOOKING HOLDINGS",   "ISRG":  "INTUITIVE SURGICAL",
    "VRTX": "VERTEX PHARMA",       "SBUX":  "STARBUCKS",
    "ADBE": "ADOBE",               "ADP":   "AUTOMATIC DATA",
    "INTU": "INTUIT",              "DDOG":  "DATADOG",
    "ENPH": "ENPHASE ENERGY",      "CCJ":   "CAMECO",
    "SMR":  "NUSCALE POWER",       "NNE":   "NANO NUCLEAR",
    "OKLO": "OKLO INC",            "WULF":  "TERAWULF",
    "UUUU": "ENERGY FUELS",        "DNN":   "DENISON MINES",
    "IDR":  "IDAHO STRATEGIC",     "TMC":   "TMC THE METALS",
    "MTSI": "MACOM TECH",          "ASX":   "ASE TECH",
    "UMC":  "UNITED MICRO",        "SWKS":  "SKYWORKS",
    "WOLF": "WOLFSPEED",           "ACLS":  "AXCELIS",
    "CRUS": "CIRRUS LOGIC",        "STX":   "SEAGATE",
    "FORM": "FORMFACTOR",          "CLS":   "CELESTICA",
}

# In-memory XML cache — downloaded once per institution per backfill run,
# reused across multiple universe scans.
_xml_cache: dict[str, str | None] = {}


# ── EDGAR submissions-API helpers ─────────────────────────────────────────────

def _padded_cik(cik: int) -> str:
    return str(cik).zfill(10)


def _get_recent_13fs(cik: int, start_date: str, max_filings: int = 2) -> list[dict]:
    """
    Fetch up to max_filings most recent 13F-HR (or 13F-HR/A) within the lookback window.
    Returns a list newest-first; may return fewer if fewer exist in the window.
    """
    url = f"{SUBMISSIONS_BASE}/CIK{_padded_cik(cik)}.json"
    resp = _sec_get(url, timeout=15)
    if not resp or not resp.ok:
        logger.debug("Submissions fetch failed for CIK %d: %s", cik, resp.status_code if resp else "no resp")
        return []

    try:
        data = resp.json()
    except Exception as exc:
        logger.debug("JSON parse error for CIK %d: %s", cik, exc)
        return []

    filer_name = data.get("name", f"CIK {cik}")
    recent     = (data.get("filings") or {}).get("recent") or {}
    forms      = recent.get("form",            [])
    dates      = recent.get("filingDate",      [])
    accessions = recent.get("accessionNumber", [])
    periods    = recent.get("reportDate",      [])

    results = []
    for form, date, acc, period in zip(forms, dates, accessions, periods):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        if date < start_date:
            break   # filing history is newest-first; past the window now
        results.append({
            "filer":      filer_name,
            "accession":  acc,
            "filed_date": date,
            "period":     _format_period(period or date),
            "cik":        cik,
        })
        if len(results) >= max_filings:
            break

    return results


def _get_info_table_xml(cik: int, accession: str) -> str | None:
    """
    Download the 13F information table XML for a filing.
    Uses the filing index to locate the correct XML file.
    Results cached in _xml_cache to avoid re-downloading across universe scans.
    """
    cache_key = accession
    if cache_key in _xml_cache:
        return _xml_cache[cache_key]

    result = None
    acc_nodash = accession.replace("-", "")

    try:
        # Fetch the filing's index page to find the info table file
        index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_nodash}/{accession}-index.htm"
        idx = _sec_get(index_url, extra_headers={"Accept": "text/html"}, timeout=12)
        if not idx or not idx.ok:
            logger.warning("  Filing index not found: %s (CIK %d)", accession, cik)
            _xml_cache[cache_key] = None
            return None

        # Find XML links in the index — prefer files with "info" or "table" in name
        xml_links = re.findall(
            r'href="(/Archives/edgar/data/[^"]+\.xml)"',
            idx.text, re.IGNORECASE
        )
        if not xml_links:
            logger.warning("  No XML files in filing index for %s", accession)
            _xml_cache[cache_key] = None
            return None

        preferred = [l for l in xml_links if re.search(r'info|table|holding', l, re.IGNORECASE)]
        link = preferred[0] if preferred else xml_links[-1]
        logger.info("  Info table file: %s", link.split("/")[-1])

        xml_resp = _sec_get(
            EDGAR_BASE + link,
            extra_headers={"Accept": "application/xml,text/xml"},
            timeout=45,
        )
        if xml_resp and xml_resp.ok and len(xml_resp.text) > 500:
            result = xml_resp.text
            logger.info("  Downloaded %d bytes of XML", len(result))
        else:
            logger.warning("  XML download failed or empty for %s", accession)

    except Exception as exc:
        logger.debug("get_info_table_xml error (%s): %s", accession, exc)

    _xml_cache[cache_key] = result
    return result


def _parse_holdings(xml_text: str, universe_tickers: set) -> list[dict]:
    """
    Parse 13F information table XML and return holdings matching universe_tickers.
    Matches by company name fragment (TICKER_NAME_MAP) since 13F uses nameOfIssuer,
    not ticker symbols.
    """
    # Build lookup: name fragment (upper) → ticker
    name_to_ticker: dict[str, str] = {}
    for ticker in universe_tickers:
        frag = TICKER_NAME_MAP.get(ticker.upper())
        if frag:
            name_to_ticker[frag.upper()] = ticker.upper()

    results = []
    blocks = re.findall(r'<infoTable>(.*?)</infoTable>', xml_text, re.DOTALL | re.IGNORECASE)

    for block in blocks:
        name_m = re.search(r'<nameOfIssuer>(.*?)</nameOfIssuer>', block, re.IGNORECASE)
        if not name_m:
            continue
        issuer = name_m.group(1).strip().upper()

        matched_ticker = None
        for frag, ticker in name_to_ticker.items():
            if frag in issuer:
                matched_ticker = ticker
                break
        if not matched_ticker:
            continue

        # Skip options
        if re.search(r'<putCall>\s*(?:put|call)\s*</putCall>', block, re.IGNORECASE):
            continue

        shares_m = re.search(r'<sshPrnamt>(\d+)</sshPrnamt>', block, re.IGNORECASE)
        value_m  = re.search(r'<value>(\d+)</value>',          block, re.IGNORECASE)

        results.append({
            "ticker":  matched_ticker,
            "shares":  int(shares_m.group(1)) if shares_m else None,
            "value_k": int(value_m.group(1))  if value_m  else None,
        })

    return results


def _format_period(period_raw: str) -> str:
    """Convert EDGAR period_of_report (YYYY-MM-DD) to 'Q2 2026' style."""
    if not period_raw or len(period_raw) < 7:
        return period_raw or "—"
    try:
        year  = int(period_raw[:4])
        month = int(period_raw[5:7])
        return f"Q{(month - 1) // 3 + 1} {year}"
    except Exception:
        return period_raw


# ── Batch scan ────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, days: int = 270, enrich: bool = True) -> list[dict]:
    """
    Find which top institutional investors hold the given tickers, based on
    their most recent 13F-HR quarterly filing.

    Compares the current filing against the prior-quarter baseline stored in
    inst_holdings (seeded via backfill_prior_holdings). After comparison,
    overwrites inst_holdings with the current quarter so it becomes the
    reference point for the next quarter's scan.
    """
    start_date  = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    ticker_set  = {t.upper() for t in tickers}
    all_results = []

    for display_name, cik in TOP_INSTITUTIONS:
        logger.info("Checking %s (CIK %d) for %d tickers...", display_name, cik, len(ticker_set))

        filings = _get_recent_13fs(cik, start_date, max_filings=1)
        if not filings:
            logger.info("  No 13F-HR found within lookback window")
            continue

        filing = filings[0]
        logger.info("  Filing: %s  filed %s  (%s)",
                    filing["filer"], filing["filed_date"], filing["period"])

        xml = _get_info_table_xml(cik, filing["accession"])
        if not xml:
            logger.warning("  Could not retrieve info table XML — skipping")
            continue

        holdings = _parse_holdings(xml, ticker_set)
        logger.info("  Matched %d holdings from universe", len(holdings))

        # ── Aggregate across sub-entities ────────────────────────────────────
        # Same ticker can appear multiple times when a filer has multiple funds
        agg: dict[str, dict] = {}
        for h in holdings:
            key = h["ticker"]
            if key in agg:
                agg[key]["shares"]  = (agg[key]["shares"]  or 0) + (h["shares"]  or 0)
                agg[key]["value_k"] = (agg[key]["value_k"] or 0) + (h["value_k"] or 0)
            else:
                agg[key] = {
                    "ticker":     h["ticker"],
                    "filer":      filing["filer"],
                    "form":       "13F-HR",
                    "filed_date": filing["filed_date"],
                    "period":     filing["period"],
                    "shares":     h["shares"] or 0,
                    "value_k":    h["value_k"] or 0,
                    "accession":  filing["accession"],
                }

        # ── Change signal: compare current filing to prior-quarter DB baseline ──
        db_prior = db.get_prior_holdings(cik)
        # Ignore same-period entries — avoids comparing a quarter to itself if
        # backfill hasn't been run yet or the scan is re-run within the same quarter
        prior = {t: v for t, v in db_prior.items() if v.get("period") != filing["period"]}

        for ticker_key, row in agg.items():
            prev = prior.get(ticker_key)
            curr_shares = row.get("shares") or 0

            if prev is None:
                row["change"]       = "Initiated"
                row["prev_shares"]  = None
                row["prev_period"]  = None
                row["shares_delta"] = None
            else:
                prev_shares = prev.get("shares") or 0
                delta = curr_shares - prev_shares
                if delta > 0:
                    row["change"] = "Added"
                elif delta < 0:
                    row["change"] = "Reduced"
                else:
                    row["change"] = "Unchanged"
                row["prev_shares"]  = prev_shares
                row["prev_period"]  = prev.get("period")
                row["shares_delta"] = delta

        # ── Detect exits: in prior but absent from current filing ─────────────
        for ticker_key, prev in prior.items():
            if ticker_key in ticker_set and ticker_key not in agg:
                all_results.append({
                    "ticker":       ticker_key,
                    "filer":        filing["filer"],
                    "form":         "13F-HR",
                    "filed_date":   filing["filed_date"],
                    "period":       filing["period"],
                    "shares":       0,
                    "value_k":      0,
                    "accession":    filing["accession"],
                    "change":       "Exited",
                    "prev_shares":  prev.get("shares"),
                    "prev_period":  prev.get("period"),
                    "shares_delta": -(prev.get("shares") or 0),
                })

        # ── Persist current holdings — becomes the baseline for next quarter ──
        now_str = datetime.now(timezone.utc).isoformat()
        holdings_to_store = [
            {
                "ticker":     row["ticker"],
                "shares":     row.get("shares"),
                "value_k":    row.get("value_k"),
                "period":     filing["period"],
                "filed_date": filing["filed_date"],
            }
            for row in agg.values()
        ]
        # NOTE: do NOT write back to inst_holdings here.
        # inst_holdings is the prior-quarter baseline managed exclusively by
        # backfill_prior_holdings(). Writing current-quarter data here would
        # overwrite the baseline and cause every subsequent scan to show
        # INITIATED for all positions.

        for row in agg.values():
            all_results.append(row)

        time.sleep(0.5)

    logger.info("scan_tickers complete: %d results across %d institutions checked",
                len(all_results), len(TOP_INSTITUTIONS))
    return all_results


def backfill_prior_holdings(tickers: list, days: int = 270) -> dict:
    """
    Seed inst_holdings with the second-most-recent 13F per institution.
    This establishes the prior-quarter baseline so that the next scan can
    derive meaningful change signals (Initiated / Added / Reduced / Exited).

    Quarter-agnostic: always uses filing[1] relative to the most recent filing,
    so it works correctly regardless of which quarter we are currently in.
    Uses seed_prior_holdings (unconditional upsert) to force-reset the baseline
    even if a scan has already run and stored the current quarter.
    """
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    ticker_set = {t.upper() for t in tickers}
    seeded = 0
    skipped = 0

    for display_name, cik in TOP_INSTITUTIONS:
        logger.info("Backfill prior: %s (CIK %d)", display_name, cik)

        filings = _get_recent_13fs(cik, start_date, max_filings=2)
        if len(filings) < 2:
            logger.info("  Only %d filing(s) in window — skipping", len(filings))
            skipped += 1
            continue

        prior_filing = filings[1]   # second most recent = prior quarter
        current_filing = filings[0]

        if prior_filing["period"] == current_filing["period"]:
            logger.info("  Prior and current have same period (%s) — skipping",
                        prior_filing["period"])
            skipped += 1
            continue

        logger.info("  Prior quarter: %s  filed %s",
                    prior_filing["period"], prior_filing["filed_date"])

        xml = _get_info_table_xml(cik, prior_filing["accession"])
        if not xml:
            logger.warning("  Could not retrieve prior XML — skipping")
            skipped += 1
            continue

        holdings = _parse_holdings(xml, ticker_set)
        if not holdings:
            logger.info("  No universe tickers found in prior filing — skipping")
            skipped += 1
            continue

        # Aggregate sub-entities
        agg: dict[str, dict] = {}
        for h in holdings:
            key = h["ticker"]
            if key in agg:
                agg[key]["shares"]  = (agg[key]["shares"]  or 0) + (h["shares"]  or 0)
                agg[key]["value_k"] = (agg[key]["value_k"] or 0) + (h["value_k"] or 0)
            else:
                agg[key] = {"shares": h["shares"] or 0, "value_k": h["value_k"] or 0}

        to_seed = [
            {
                "ticker":     ticker,
                "shares":     d["shares"],
                "value_k":    d["value_k"],
                "period":     prior_filing["period"],
                "filed_date": prior_filing["filed_date"],
            }
            for ticker, d in agg.items()
        ]

        db.seed_prior_holdings(cik, to_seed, datetime.now(timezone.utc).isoformat())
        logger.info("  Seeded %d prior holdings (%s)", len(to_seed), prior_filing["period"])
        seeded += len(to_seed)
        time.sleep(0.5)

    logger.info("backfill_prior_holdings complete: %d holdings seeded, %d institutions skipped",
                seeded, skipped)
    return {"seeded": seeded, "skipped": skipped}
