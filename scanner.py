"""
inst-scanner/scanner.py
SEC EDGAR SC 13D/13G filing fetcher + ownership % extractor.
"""

import re
import time
import logging
import requests
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Universe definitions (mirrors the frontend) ───────────────────────────────
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
        "MU","AMD","AVGO","INTC","NVDA","MRVL","AMAT","TXN","QCOM",
        "NXPI","MPWR","LRCX","KLAC","ADI","TER","MCHP","TSM","ASML",
        "ON","ALAB","CRDO","MTSI","ENTG","STX","SWKS","WOLF","CRUS",
        "ACLS","FORM","MXL","AMBA","POWI","DIOD","AOSL",
    ],
    "smh": [
        "NVDA","TSM","AVGO","ASML","AMD","TXN","QCOM","AMAT","LRCX",
        "KLAC","MU","ADI","MRVL","INTC","NXPI","MPWR","ON","MCHP",
        "TER","STX","ENTG","SWKS","WOLF","AMBA","ACLS","CRUS",
    ],
    "ndx100": [
        "AAPL","MSFT","NVDA","AMZN","META","TSLA","GOOGL","GOOG",
        "AVGO","COST","NFLX","TMUS","AMD","PEP","CSCO","ADBE","QCOM",
        "TXN","AMGN","INTC","INTU","ISRG","BKNG","VRTX","CMCSA","MU",
        "AMAT","PANW","LRCX","REGN","KLAC","ADI","MRVL","CRWD","MDLZ",
        "CEG","CTAS","FTNT","SNPS","CDNS","MELI","ASML","CSX","ORLY",
        "MAR","ABNB","PYPL","WDAY","PCAR","MNST","ADSK","ADP","FAST",
        "ROST","KDP","DXCM","CHTR","ODFL","IDXX","CPRT","MCHP","EXC",
        "BIIB","GEHC","NXPI","VRSK","ON","DDOG","CTSH","GFS","TTD",
        "ANSS","FANG","SMCI","ARM","APP","AXON","MPWR","ZS","CRDO",
        "ALAB","NET","DASH","COIN","PLTR",
    ],
    "sp500": [
        "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO",
        "JPM","LLY","V","XOM","MA","UNH","JNJ","PG","HD","MRK","ABBV",
        "CVX","COST","CRM","BAC","NFLX","AMD","WMT","KO","PEP","ADBE",
        "TMO","MCD","CSCO","ORCL","GE","GS","ABT","T","VZ","INTC",
        "IBM","QCOM","TXN","HON","INTU","AMGN","CAT","AMAT","BKNG",
        "ISRG","NOW","SPGI","BLK","PFE","AXP","LRCX","KLAC","ADI",
        "MRVL","PANW","CRWD","REGN","VRTX","MU","DE","SYK","GILD",
        "MDLZ","CMCSA","ETN","SBUX","TMUS","PLD","MMC","CB","AON",
        "ZTS","CME","ICE","TDG","WELL","DUK","SO","NEE","AEP","EXC",
    ],
}

INST_FORMS = ["SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"]

# SEC requires a meaningful User-Agent
HEADERS = {
    "User-Agent": "PoppaAlpha/1.0 research@poppa-alpha.com",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

EFTS_URL  = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BASE = "https://www.sec.gov"


# ── Ownership % extraction ────────────────────────────────────────────────────

_PCT_PATTERNS = [
    # Row 13 / Item 13 style: "13.  5.2%"
    re.compile(r"\b1[23][\.\)]\s{0,10}(\d{1,3}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
    # "percent of class ... 5.2%"
    re.compile(r"percent\s+of\s+(?:the\s+)?class[^%\d]{0,60}(\d{1,3}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
    # "represents approximately 5.2% of"
    re.compile(r"represents\s+approximately\s+(\d{1,3}(?:\.\d{1,2})?)\s*%\s+of", re.IGNORECASE),
    # generic "beneficial ownership of 5.2%"
    re.compile(r"beneficial\s+ownership\s+of\s+(\d{1,3}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
]


def _extract_ownership_from_text(text: str):
    for pat in _PCT_PATTERNS:
        m = pat.search(text)
        if m:
            val = float(m.group(1))
            if 0.1 <= val <= 99.9:     # sanity: must be a plausible %
                return round(val, 2)
    return None


def _fetch_filing_document(accession_id: str) -> str | None:
    """
    Try to fetch the primary filing document text for an SC 13D/13G
    and return its raw text. Returns None if it can't be retrieved.

    Accession IDs from EFTS look like '0001234567-26-000123'.
    """
    try:
        # Build the index page URL.
        # CIK is embedded as the first 10 digits of the accession number.
        acc_clean = accession_id.replace("-", "")
        cik       = str(int(acc_clean[:10]))          # strip leading zeros
        acc_path  = f"{acc_clean[:10]}-{acc_clean[10:12]}-{acc_clean[12:]}"
        index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_path}-index.htm"

        resp = requests.get(index_url, headers=HEADERS, timeout=12)
        if not resp.ok:
            return None

        # Find the primary document link (not the index itself)
        doc_links = re.findall(
            r'href="(/Archives/edgar/data/[^"]+\.(?:htm|html|txt))"',
            resp.text,
            re.IGNORECASE,
        )
        # Skip the index itself, prefer the shortest path (usually primary doc)
        doc_links = [l for l in doc_links if "-index" not in l]
        if not doc_links:
            return None

        doc_url  = EDGAR_BASE + doc_links[0]
        doc_resp = requests.get(doc_url, headers=HEADERS, timeout=15)
        if not doc_resp.ok:
            return None

        # Strip HTML tags for cleaner regex matching
        text = re.sub(r"<[^>]+>", " ", doc_resp.text)
        text = re.sub(r"\s+", " ", text)
        return text

    except Exception as exc:
        logger.debug("fetch_filing_document error: %s", exc)
        return None


def extract_ownership(accession_id: str) -> float | None:
    """Fetch the filing and try to extract ownership percentage."""
    doc_text = _fetch_filing_document(accession_id)
    if doc_text:
        return _extract_ownership_from_text(doc_text)
    return None


# ── EDGAR EFTS search ─────────────────────────────────────────────────────────

def fetch_filings_for_ticker(ticker: str, start_date: str) -> list[dict]:
    """
    Search EDGAR EFTS for 13D/13G filings mentioning `ticker`
    filed on or after `start_date` (YYYY-MM-DD).
    Returns a list of filing dicts.
    """
    forms_param = ",".join(INST_FORMS)
    params = {
        "q":          f'"{ticker}"',
        "forms":      forms_param,
        "dateRange":  "custom",
        "startdt":    start_date,
    }
    try:
        resp = requests.get(EFTS_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("EDGAR search error for %s: %s", ticker, exc)
        return []

    hits = (data.get("hits") or {}).get("hits") or []
    filings = []
    for h in hits:
        src  = h.get("_source") or {}
        form = src.get("form_type", "")
        if form not in INST_FORMS:
            continue

        # Company name from display_names (may list multiple, take first)
        display = src.get("display_names") or ""
        company = display.split(";")[0].strip() if display else "—"

        accession = (h.get("_id") or "").replace(":", "-")

        filings.append({
            "ticker":        ticker,
            "company":       company,
            "form":          form,
            "filer":         src.get("entity_name") or "—",
            "ownership_pct": None,          # filled below
            "filed_date":    src.get("file_date") or "—",
            "accession":     accession,
        })

    return filings


def enrich_ownership(filing: dict, delay: float = 0.4) -> dict:
    """Attempt to fill in ownership_pct by fetching the actual filing doc."""
    time.sleep(delay)
    try:
        pct = extract_ownership(filing["accession"])
        filing["ownership_pct"] = pct
    except Exception as exc:
        logger.debug("ownership extraction failed (%s): %s", filing["accession"], exc)
    return filing


# ── Batch scan ────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, days: int = 90, enrich: bool = True) -> list[dict]:
    """
    Scan a list of tickers for recent 13D/13G filings.
    If `enrich` is True, attempt ownership % extraction for each filing.
    """
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).strftime("%Y-%m-%d")

    all_filings = []
    for sym in tickers:
        filings = fetch_filings_for_ticker(sym, start_date)
        if filings:
            logger.info("  %s: %d filing(s)", sym, len(filings))
            if enrich:
                filings = [enrich_ownership(f) for f in filings]
        else:
            logger.info("  %s: no filings", sym)
        all_filings.extend(filings)
        time.sleep(0.25)    # be polite to SEC servers

    return all_filings
