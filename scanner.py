"""
inst-scanner/scanner.py
SEC EDGAR 13F-HR institutional holdings scanner.

For each ticker, searches EDGAR full-text search (EFTS) for recent 13F-HR
quarterly filings filed by institutions that hold that stock.
13F-HR is filed quarterly by every fund manager with >$100M AUM, listing
all equity holdings — this is the standard institutional ownership dataset.
"""

import os
import re
import time
import logging
import requests
from datetime import datetime, timedelta, timezone

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
    # Nasdaq-100 as of Aug 7 2026 (slickcharts.com) — post June 2026 rebalance
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
    # S&P 500
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

INST_FORMS = ["13F-HR", "13F-HR/A"]

# SEC requires a meaningful User-Agent — read from env var set in Render environment group
_SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "OspreyResearch/1.0 research@osprey.com")
HEADERS = {
    "User-Agent": _SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

EFTS_URL   = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BASE = "https://www.sec.gov"

# Max institutional filers to return per ticker (13F filings are very common
# for large-cap stocks — capping prevents returning hundreds of results)
MAX_FILERS_PER_TICKER = 20

# Minimum shares threshold — filter out trivially small positions
MIN_SHARES = 1000


# ── EDGAR EFTS search ─────────────────────────────────────────────────────────

def fetch_filings_for_ticker(ticker: str, start_date: str) -> list[dict]:
    """
    Search EDGAR EFTS for 13F-HR filings mentioning ticker, filed on or
    after start_date (YYYY-MM-DD).

    13F-HR filings are quarterly institutional ownership reports. Many filers
    include the ticker symbol in their information table, so EFTS full-text
    search finds them reliably for most tickers.

    Returns a list of institutional holder dicts sorted by filed_date desc.
    """
    params = {
        "q":         f'"{ticker}"',
        "forms":     "13F-HR,13F-HR/A",
        "dateRange": "custom",
        "startdt":   start_date,
        "from":      0,
        "size":      MAX_FILERS_PER_TICKER,
    }
    try:
        resp = requests.get(EFTS_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("EDGAR EFTS error for %s: %s", ticker, exc)
        return []

    hits  = (data.get("hits") or {}).get("hits") or []
    total = (data.get("hits") or {}).get("total", {}).get("value", 0)

    results = []
    for h in hits:
        src  = h.get("_source") or {}
        form = src.get("form_type", "")
        if form not in INST_FORMS:
            continue

        period_raw = src.get("period_of_report") or ""
        # Format period as "Q2 2026" style
        period_label = _format_period(period_raw)

        results.append({
            "ticker":      ticker,
            "filer":       src.get("entity_name") or "—",
            "form":        form,
            "filed_date":  src.get("file_date") or "—",
            "period":      period_label,
            "period_raw":  period_raw,
            "shares":      None,   # populated by enrich step (optional)
            "value_k":     None,   # populated by enrich step (optional)
            "accession":   (h.get("_id") or "").replace(":", "-"),
            "total_filers": total,
        })

    return results


def _format_period(period_raw: str) -> str:
    """Convert EDGAR period_of_report (YYYY-MM-DD) to 'Q2 2026' style."""
    if not period_raw or len(period_raw) < 7:
        return period_raw or "—"
    try:
        year  = int(period_raw[:4])
        month = int(period_raw[5:7])
        q = (month - 1) // 3 + 1
        return f"Q{q} {year}"
    except Exception:
        return period_raw


# ── Batch scan ────────────────────────────────────────────────────────────────

def scan_tickers(tickers: list, days: int = 90, enrich: bool = True) -> list[dict]:
    """
    Scan a list of tickers for recent 13F-HR institutional filings.

    For each ticker, returns up to MAX_FILERS_PER_TICKER institutional filers
    that recently filed a 13F-HR mentioning that ticker.

    Note: enrich parameter kept for API compatibility but position-size
    extraction is not yet implemented (requires downloading each filing XML).
    """
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).strftime("%Y-%m-%d")

    all_filings = []
    for sym in tickers:
        filings = fetch_filings_for_ticker(sym, start_date)
        if filings:
            total = filings[0].get("total_filers", len(filings))
            logger.info("  %s: %d filers (showing %d of %d)", sym, len(filings), len(filings), total)
        else:
            logger.info("  %s: no filings", sym)
        all_filings.extend(filings)
        time.sleep(0.3)   # be polite to SEC EDGAR

    return all_filings
