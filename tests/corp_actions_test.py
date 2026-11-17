"""13F corporate actions in holdings_view: splits, CUSIP changes, merger flag, filings reported in thousands.
Runs on SQLite (no DATABASE_URL)."""
import os
import sys
import tempfile

os.environ.pop("DATABASE_URL", None)
os.environ["INST_SQLITE_PATH"] = os.path.join(tempfile.mkdtemp(), "t.db")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import holdings as H  # noqa: E402

H.init_tables()
Q2, Q3 = ("Q2 2026", "2026-06-30"), ("Q3 2026", "2026-09-30")
MGRS = [(100 + i, f"MANAGER {i}") for i in range(6)]
px = {"APH": (120.0, 130.0), "XOM": (110.0, 115.0), "EQR": (70.0, 70.0), "WFC": (80.0, 82.0), "AMD": (580.0, 612.0)}
CUSIP = {"APH": "032095AA", "XOM": "30231G1N", "EQR": "29476L10", "WFC": "94974610", "AMD": "00790310"}
OLD_XOM, WFC_PREF = "30231G10", "94974680"
with H._DB() as d:
    for t, c8 in CUSIP.items():
        d.q("INSERT INTO ticker_cusip (ticker, cusip, cusip8, name, updated_at) VALUES (%s,%s,%s,%s,%s)",
            (t, c8 + "0", c8, t, "2026-11-01T00:00:00+00:00"))
    d.q("INSERT INTO ticker_splits (ticker, split_date, numerator, denominator) VALUES (%s,%s,%s,%s)", ("APH", "2026-07-15", 2, 1))
    for cik, name in MGRS:
        thousands = cik == 105                       # this manager files values in $ thousands
        for (per, end), qi in ((Q2, 0), (Q3, 1)):
            acc = f"{cik}-{qi}"
            pos = []
            base = 1_000_000 * (cik - 99)
            sh = {"APH": base * (1 if qi == 0 else 2 + (0.25 if cik == 100 else 0)),   # 2:1 split; mgr 100 also buys
                  "EQR": base * (1 if qi == 0 else 2.1),                                  # stock merger, no split on record
                  "WFC": base, "AMD": base * (1 if qi == 0 else 1.1)}
            for t, n in sh.items():
                pos.append((acc, cik, end, CUSIP[t] + "0", CUSIP[t], t + " INC", "COM", n, n * px[t][qi]))
            # XOM filed under the old CUSIP in Q2 and the new one in Q3
            xc = OLD_XOM if qi == 0 else CUSIP["XOM"]
            pos.append((acc, cik, end, xc + "0", xc, "EXXON MOBIL CORP", "COM", base, base * px["XOM"][qi]))
            # a WFC preferred under the same issuer name, in the same filings as the common
            pos.append((acc, cik, end, WFC_PREF + "0", WFC_PREF, "WFC INC", "PFD", 1000, 25000))
            if thousands:
                pos = [p[:8] + (p[8] / 1000,) for p in pos]
            tot = sum(p[8] for p in pos)
            d.q("""INSERT INTO inst_filings (accession, cik, filer, form, period_end, period, filed_date, total_value,
                   n_positions, top10_pct, tier, ingested_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (acc, cik, name, "13F-HR", end, per, end, tot, len(pos), 50.0, "diversified", "x"))
            for p in pos:
                d.q("""INSERT INTO inst_positions (accession, cik, period_end, cusip, cusip8, issuer, title, shares, value)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", p)

rows = H.holdings_view(set(CUSIP), "Q3 2026")
by = lambda t: [r for r in rows if r["ticker"] == t]

# splits: holders who only went through the 2:1 are Unchanged; manager 100 really bought 0.25x pre-split base
aph = by("APH")
assert {r["change"] for r in aph if r["cik"] != 100} == {"Unchanged"}, [r["change"] for r in aph]
m100 = next(r for r in aph if r["cik"] == 100)
assert m100["change"] == "Added" and abs(m100["shares_delta"] - 250_000) < 1 and m100["split_factor"] == 2.0
assert not any(r.get("corp_action") for r in aph)

# CUSIP change: XOM links to Q2 under the old CUSIP — no fake initiations
xom = by("XOM")
assert {r["change"] for r in xom} == {"Unchanged"}, [r["change"] for r in xom]
# the preferred (same issuer, same filings as the common) is never merged into WFC
assert all(r["shares"] == r["prev_shares"] for r in by("WFC"))

# merger with no split on record: flagged
eqr = by("EQR")
assert eqr and all("merger" in (r.get("corp_action") or "") for r in eqr)
# ordinary buying is not flagged (AMD: every holder +10%)
assert not any(r.get("corp_action") for r in by("AMD"))

# the manager filing in thousands is rescaled: implied AMD price ~612, not 0.612
amd105 = next(r for r in by("AMD") if r["cik"] == 105)
assert abs(amd105["value"] / amd105["shares"] - 612) < 1, amd105["value"] / amd105["shares"]
assert abs(amd105["prev_value"] / amd105["prev_shares"] - 580) < 1

assert H.split_factor([("2026-07-15", 2, 1), ("2025-01-10", 1, 10)], "2026-06-30", "2026-09-30") == 2.0
print("CORP ACTIONS OK")
