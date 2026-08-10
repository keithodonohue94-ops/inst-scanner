"""
inst-scanner/scanner.py
SEC EDGAR 13F-HR institutional holdings scanner — institution-first approach.

EDGAR EFTS full-text search does NOT index 13F holdings data (the info table
XML is not text-indexed). The only reliable approach is:
  1. Search EDGAR for each top institution's 13F-HR filing by filer name
  2. Download the filing's information table XML
  3. Parse holdings and filter for our universe tickers

Each institution files ONE 13F-HR per quarter listing ALL their equity holdings.
We check the top ~20 institutions and match their holdings to our universe tickers.
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

# ── SEC EDGAR config ──────────────────────────────────────────────────────────

_SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "OspreyResearch/1.0 research@osprey.com")
HEADERS = {
    "User-Agent": _SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

EFTS_URL   = "https://efts.sec.gov/LATEST/search-index"
EDGAR_BASE = "https://www.sec.gov"

# ── Top institutions to check ─────────────────────────────────────────────────
# Search terms used to find each institution's 13F filing via EDGAR EFTS.
# These match the entity_name field in EDGAR filings.
TOP_INSTITUTIONS = [
    "VANGUARD GROUP",
    "BLACKROCK",
    "STATE STREET",
    "FIDELITY MANAGEMENT",
    "T ROWE PRICE",
    "JPMORGAN CHASE",
    "GOLDMAN SACHS",
    "MORGAN STANLEY",
    "INVESCO",
    "GEODE CAPITAL",
    "NORTHERN TRUST",
    "WELLINGTON MANAGEMENT",
    "CAPITAL RESEARCH",
    "CITADEL ADVISORS",
    "MILLENNIUM MANAGEMENT",
    "RENAISSANCE TECHNOLOGIES",
    "TWO SIGMA",
    "AQR CAPITAL",
    "COATUE MANAGEMENT",
    "TIGER GLOBAL",
    "POINT72",
    "DE SHAW",
    "BRIDGEWATER",
    "BALYASNY",
    "VIKING GLOBAL",
]

# Ticker → fragment of company name as it appears in 13F nameOfIssuer field.
# Used to match holdings back to our universe tickers.
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
    "LITE": "LUMENTUM",            "ENPH":  "ENPHASE ENERGY",
    "FSLR": "FIRST SOLAR",         "NEE":   "NEXTERA ENERGY",
    "CEG":  "CONSTELLATION ENERGY","VST":   "VISTRA",
    "NRG":  "NRG ENERGY",          "PLUG":  "PLUG POWER",
    "RUN":  "SUNRUN",              "MP":    "MP MATERIALS",
    "RKLB": "ROCKET LAB",          "ASTS":  "AST SPACEMOBILE",
    "LUNR": "INTUITIVE MACHINES",  "BKSY":  "BLACKSKY",
    "PL":   "PLANET LABS",         "SPCE":  "VIRGIN GALACTIC",
    "RDW":  "REDWIRE",             "S":     "SENTINELONE",
    "OKTA": "OKTA",                "ZS":    "ZSCALER",
    "NET":  "CLOUDFLARE",          "FTNT":  "FORTINET",
    "CYBR": "CYBERARK",            "TENB":  "TENABLE",
    "QLYS": "QUALYS",              "NVDA":  "NVIDIA",
    "WMT":  "WALMART",             "COST":  "COSTCO",
    "NFLX": "NETFLIX",             "AMGN":  "AMGEN",
    "SHOP": "SHOPIFY",             "TMUS":  "T-MOBILE",
    "PEP":  "PEPSICO",             "GILD":  "GILEAD",
    "BKNG": "BOOKING HOLDINGS",   "ISRG":  "INTUITIVE SURGICAL",
    "VRTX": "VERTEX PHARMA",       "SBUX":  "STARBUCKS",
    "ADBE": "ADOBE",               "ADP":   "AUTOMATIC DATA",
    "INTU": "INTUIT",              "CSX":   "CSX CORP",
    "MAR":  "MARRIOTT",            "CMCSA": "COMCAST",
    "DDOG": "DATADOG",             "ROST":  "ROSS STORES",
    "CTAS": "CINTAS",              "REGN":  "REGENERON",
    "HON":  "HONEYWELL",           "ORLY":  "O REILLY AUTO",
    "PCAR": "PACCAR",              "AEP":   "AMERICAN ELECTRIC",
    "FAST": "FASTENAL",            "FANG":  "DIAMONDBACK",
    "ADSK": "AUTODESK",            "PYPL":  "PAYPAL",
    "XEL":  "XCEL ENERGY",         "EXC":   "EXELON",
    "IDXX": "IDEXX LAB",           "AXON":  "AXON ENTERPRISE",
    "TTWO": "TAKE TWO",            "ODFL":  "OLD DOMINION",
    "WDAY": "WORKDAY",             "PAYX":  "PAYCHEX",
    "KDP":  "KEURIG DR PEPPER",    "ROP":   "ROPER TECH",
    "GEHC": "GE HEALTHCARE",       "DXCM":  "DEXCOM",
    "ALNY": "ALNYLAM",             "CPRT":  "COPART",
    "MTSI": "MACOM TECH",          "ASX":   "ASE TECH",
    "UMC":  "UNITED MICRO",        "SWKS":  "SKYWORKS",
    "WOLF": "WOLFSPEED",           "ACLS":  "AXCELIS",
    "CRUS": "CIRRUS LOGIC",        "STX":   "SEAGATE",
    "FORM": "FORMFACTOR",
}

# In-memory cache for institution 13F XML — downloaded once per backfill run,
# reused across multiple universe scans.
_xml_cache: dict[str, str | None] = {}


# ── EDGAR helpers ─────────────────────────────────────────────────────────────

def _find_institution_13f(institution_name: str, start_date: str) -> dict | None:
    """
    Find the most recent 13F-HR filing for an institution by searching EDGAR
    EFTS for filings where the entity_name matches the institution name.

    Returns a dict with cik, accession, filer, filed_date, period or None.
    """
    params = {
        "q":         f'"{institution_name}"',
        "forms":     "13F-HR",
        "dateRange": "custom",
        "startdt":   start_date,
        "from":      0,
        "size":      5,
    }
    try:
        resp = requests.get(EFTS_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("EFTS search error for %s: %s", institution_name, exc)
        return None

    hits = (data.get("hits") or {}).get("hits") or []
    name_parts = institution_name.upper().split()

    for hit in hits:
        src         = hit.get("_source") or {}
        entity_name = (src.get("entity_name") or "").upper()
        # Confirm this hit is actually the institution filing (not a mention)
        if not any(part in entity_name for part in name_parts[:2]):
            continue

        accession = (hit.get("_id") or "").replace(":", "-")
        acc_clean = accession.replace("-", "")
        cik       = str(int(acc_clean[:10]))   # strip leading zeros

        return {
            "cik":        cik,
            "accession":  accession,
            "filer":      src.get("entity_name") or institution_name,
            "filed_date": src.get("file_date") or "—",
            "period":     _format_period(src.get("period_of_report") or ""),
        }
    return None


def _get_info_table_xml(cik: str, accession: str) -> str | None:
    """
    Download the 13F information table XML for a filing.
    Fetches the filing index to find the info table file URL.
    Results are cached in _xml_cache to avoid redundant downloads.
    """
    if accession in _xml_cache:
        return _xml_cache[accession]

    result = None
    try:
        index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{accession}-index.htm"
        resp = requests.get(index_url, headers={**HEADERS, "Accept": "text/html"}, timeout=12)
        if not resp.ok:
            _xml_cache[accession] = None
            return None

        # Find the information table XML link
        # Prioritise files with "infotable" or "information" in the name
        xml_links = re.findall(
            r'href="(/Archives/edgar/data/[^"]+\.xml)"',
            resp.text, re.IGNORECASE
        )
        if not xml_links:
            _xml_cache[accession] = None
            return None

        # Prefer the info table file over the primary submission wrapper
        preferred = [l for l in xml_links if re.search(r'info|table|holding', l, re.IGNORECASE)]
        link = preferred[0] if preferred else xml_links[-1]

        xml_resp = requests.get(
            EDGAR_BASE + link,
            headers={**HEADERS, "Accept": "application/xml,text/xml"},
            timeout=30,
        )
        result = xml_resp.text if xml_resp.ok else None

    except Exception as exc:
        logger.debug("get_info_table_xml error (%s): %s", accession, exc)

    _xml_cache[accession] = result
    return result


def _parse_holdings(xml_text: str, universe_tickers: set) -> list[dict]:
    """
    Parse a 13F information table XML and return holdings that match
    tickers in universe_tickers.

    Matching is done by company name fragment (TICKER_NAME_MAP) since
    13F XML uses nameOfIssuer, not ticker symbols.
    """
    # Build lookup: name fragment → ticker (only for tickers we care about)
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

        # Skip options (put/call entries)
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
    their most recent 13F-HR quarterly filings.

    For each of TOP_INSTITUTIONS:
      1. Find their most recent 13F-HR filing via EDGAR EFTS
      2. Download and parse the information table XML (cached across calls)
      3. Match holdings to tickers in the provided list

    Returns list of {ticker, filer, shares, value_k, period, filed_date}.
    """
    start_date   = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    ticker_set   = {t.upper() for t in tickers}
    all_results  = []

    for inst_name in TOP_INSTITUTIONS:
        logger.info("Checking %s for %d tickers...", inst_name, len(ticker_set))
        filing = _find_institution_13f(inst_name, start_date)
        if not filing:
            logger.info("  No 13F found for %s in lookback window", inst_name)
            time.sleep(0.5)
            continue

        logger.info("  Found: %s (filed %s, %s)", filing["filer"], filing["filed_date"], filing["period"])

        xml = _get_info_table_xml(filing["cik"], filing["accession"])
        if not xml:
            logger.warning("  Could not retrieve info table XML for %s", filing["filer"])
            time.sleep(0.5)
            continue

        holdings = _parse_holdings(xml, ticker_set)
        logger.info("  Matched %d holdings from universe", len(holdings))

        for h in holdings:
            all_results.append({
                "ticker":     h["ticker"],
                "filer":      filing["filer"],
                "form":       "13F-HR",
                "filed_date": filing["filed_date"],
                "period":     filing["period"],
                "shares":     h["shares"],
                "value_k":    h["value_k"],
                "accession":  filing["accession"],
            })

        time.sleep(1.0)   # be polite to SEC EDGAR

    logger.info("scan_tickers complete: %d results across %d institutions",
                len(all_results), len(TOP_INSTITUTIONS))
    return all_results
