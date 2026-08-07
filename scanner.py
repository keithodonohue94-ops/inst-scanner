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
    # SOXX = iShares Semiconductor ETF (~31 holdings). Source: stockanalysis.com Jul 30 2026
    "soxx": [
        "AMD","NVDA","MU","AVGO","INTC","AMAT","TSM","KLAC","LRCX","TXN",
        "MRVL","ADI","MPWR","NXPI","TER","QCOM","ASML","ALAB","MCHP","CRDO",
        "ON","ASX","ENTG","MTSI","UMC","SWKS","WOLF","ACLS","CRUS","STX","FORM",
    ],
    # SMH = VanEck Semiconductor ETF (26 holdings). Source: stockanalysis.com Jul 30 2026
    "smh": [
        "NVDA","TSM","AVGO","AMD","ASML","TXN","MU","ADI","AMAT","QCOM",
        "KLAC","LRCX","INTC","MRVL","CDNS","SNPS","MPWR","TER","NXPI","STM",
        "ARM","MCHP","ALAB","ON","SWKS","WOLF",
    ],
    # Nasdaq-100 as of Aug 7, 2026 (slickcharts.com) — post June 2026 rebalance
    # Added: ALAB, CRWV, NBIS, RKLB, TER | Removed: CHTR, CTSH, INSM, VRSK, ZS
    "ndx100": [
        "NVDA","AAPL","MSFT","AMZN","GOOGL","GOOG","AVGO","META","TSLA","MU",
        "WMT","AMD","ASML","INTC","CSCO","AMAT","COST","PLTR","LRCX","NFLX",
        "ARM","PANW","TXN","KLAC","LIN","AMGN","CRWD","MRVL","SHOP","ADI",
        "TMUS","PEP","STX","SNDK","QCOM","GILD","BKNG","WDC","ISRG","PDD",
        "VRTX","SBUX","FTNT","APP","ADP","ADBE","ABNB","CEG","DASH","CSX",
        "CDNS","MAR","MELI","CMCSA","INTU","MNST","DDOG","ROST","CTAS","REGN",
        "MDLZ","SNPS","HON","ORLY","PCAR","LITE","AEP","MPWR","WBD","BKR",
        "NXPI","TER","FAST","ALAB","FANG","ADSK","HONA","RKLB","PYPL","CRWV",
        "XEL","NBIS","FER","CCEP","EXC","MCHP","IDXX","AXON","TTWO","ODFL",
        "TRI","WDAY","PAYX","KDP","ROP","MSTR","GEHC","DXCM","KHC","ALNY","CPRT",
    ],
    # S&P 500 full constituent list. Source: GitHub datasets/s-and-p-500-companies (503 tickers)
    "sp500": [
        "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A","APD","ABNB",
        "AKAM","ALB","ARE","ALGN","ALLE","LNT","ALL","GOOGL","GOOG","MO","AMZN",
        "AMCR","AEE","AEP","AXP","AIG","AMT","AWK","AMP","AME","AMGN","APH","ADI",
        "AON","APA","APO","AAPL","AMAT","APP","APTV","ACGL","ADM","ARES","ANET",
        "AJG","AIZ","T","ATO","ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL",
        "BAC","BAX","BDX","BRK.B","BBY","TECH","BIIB","BLK","BX","BNY","BA","BKNG",
        "BSX","BMY","AVGO","BR","BRO","BF.B","BLDR","BG","BXP","CHRW","CDNS","CPT",
        "COF","CAH","CCL","CARR","CVNA","CASY","CAT","CBOE","CBRE","CDW","COR","CNC",
        "CNP","CF","CRL","SCHW","CHTR","CVX","CMG","CB","CHD","CIEN","CI","CINF",
        "CTAS","CSCO","C","CFG","CLX","CME","CMS","KO","CTSH","COHR","COIN","CL",
        "CMCSA","FIX","CAG","COP","ED","STZ","CEG","COO","CPRT","GLW","CPAY","CTVA",
        "CSGP","COST","CRH","CRWD","CCI","CSX","CMI","CVS","DHR","DRI","DDOG","DVA",
        "DECK","DE","DELL","DAL","DVN","DXCM","FANG","DLR","DG","DLTR","D","DPZ",
        "DASH","DOV","DOW","DHI","DTE","DUK","DD","ETN","EBAY","ECL","EIX","EW","EA",
        "ELV","EME","EMR","ETR","EOG","EQT","EFX","EQIX","EQR","ERIE","ESS","EL","EG",
        "EVRG","ES","EXC","EXE","EXPE","EXPD","EXR","XOM","FFIV","FDS","FICO","FAST",
        "FRT","FDX","FIS","FITB","FSLR","FE","FISV","FLEX","F","FTNT","FTV","FOXA",
        "FOX","BEN","FCX","GRMN","IT","GE","GEHC","GEV","GEN","GNRC","GD","GIS","GM",
        "GPC","GILD","GPN","GL","GDDY","GS","HAL","HIG","HAS","HCA","DOC","HSIC","HSY",
        "HPE","HLT","HD","HONA","HON","HRL","HST","HWM","HPQ","HUBB","HUM","HBAN","HII",
        "IBM","IEX","IDXX","ITW","INCY","IR","PODD","INTC","IBKR","ICE","IFF","IP",
        "INTU","ISRG","IVZ","INVH","IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI",
        "JPM","KVUE","KDP","KEY","KEYS","KMB","KIM","KMI","KKR","KLAC","KHC","KR",
        "LHX","LH","LRCX","LVS","LDOS","LEN","LII","LLY","LIN","LYV","LMT","L","LOW",
        "LULU","LITE","LYB","MTB","MPC","MAR","MLM","MAS","MA","MKC","MCD","MCK","MDT",
        "MRK","META","MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","TAP","MDLZ",
        "MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP","NFLX","NEM","NWSA",
        "NWS","NEE","NKE","NI","NDSN","NSC","NTRS","NOC","NCLH","NRG","NUE","NVDA",
        "NVR","NXPI","ORLY","OXY","ODFL","OMC","ON","OKE","ORCL","OTIS","PCAR","PKG",
        "PLTR","PANW","PH","PAYX","PYPL","PNR","PEP","PFE","PCG","PM","PSX","PNW","PNC",
        "POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU","PEG","PTC","PSA","PHM","PWR",
        "QCOM","DGX","Q","RL","RJF","RTX","O","REG","REGN","RF","RSG","RMD","RVTY",
        "HOOD","ROK","ROL","ROP","ROST","RCL","SPGI","CRM","SNDK","SBAC","SLB","STX",
        "SRE","NOW","SHW","SPG","SWKS","SJM","SW","SNA","SOLV","SO","LUV","SWK","SBUX",
        "STT","STLD","STE","SYK","SMCI","SYF","SNPS","SYY","TMUS","TROW","TTWO","TPR",
        "TRGP","TGT","TEL","TDY","TER","TSLA","TXN","TPL","TXT","TMO","TJX","TKO","TTD",
        "TSCO","TT","TDG","TRV","TRMB","TFC","TYL","TSN","USB","UBER","UDR","ULTA","UNP",
        "UAL","UPS","URI","UNH","UHS","VLO","VEEV","VTR","VLTO","VRSN","VRSK","VZ",
        "VRTX","VRT","VTRS","VICI","V","VST","VMC","WRB","GWW","WAB","WMT","DIS","WBD",
        "WM","WAT","WEC","WFC","WELL","WST","WDC","WY","WSM","WMB","WTW","WDAY","WYNN",
        "XEL","XYL","YUM","ZBRA","ZBH","ZTS",
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
