"""
data_quality_report.py — coverage and conflict report for a Phase 2 run,
without writing SQL by hand.

Answers exactly the three questions that matter before trusting Phase 2's
output for real decisions:
  1. Which fields are actually coming from EDGAR (primary, as intended)
     vs falling back to yfinance vs using an approximation (e.g. the
     OCF-as-FCF proxy) vs missing entirely?
  2. Where do EDGAR and yfinance actively DISAGREE (not just "one was
     picked over the other") on the same underlying number?
  3. Which specific tickers are worst-affected, so you can spot-check
     them by hand if something looks off?

Default top-N (for the "worst tickers" and "most frequent conflicts"
sections) is set by TOP_N below — change that one line to adjust it
everywhere in this report.

Usage:
    python data_quality_report.py --run-id fundamental-LONG-2026-08-22-abc123
    python data_quality_report.py --run-id <id> --top 30
"""

import argparse
import json
import os
from collections import Counter, defaultdict

import mysql.connector

# Change this one line to adjust how many rows show in the "worst
# tickers" / "most common conflicts" sections — no need to dig through
# the rest of the file.
TOP_N = 20

# Field sources this report treats as "approximate" rather than a clean
# hit — surfaced separately from a plain 'missing', since these are
# silently degrading score quality rather than obviously absent.
PROXY_SOURCES = {"ocf-proxy"}
MISSING_SOURCES = {"missing"}


def get_connection(args):
    return mysql.connector.connect(
        host=args.host or os.environ.get("MYSQL_HOST", "localhost"),
        database=args.db or os.environ.get("MYSQL_DB", "screener"),
        user=args.user or os.environ.get("MYSQL_USER", "root"),
        password=args.password or os.environ.get("MYSQL_PASSWORD", ""),
        port=args.port or int(os.environ.get("MYSQL_PORT", "3306")),
    )


def _print_table(rows, headers):
    if not rows:
        print("  (none)")
        return
    widths = [max(len(str(h)), max(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * w for w in widths]))
    for r in rows:
        print(fmt.format(*[str(v) if v is not None else "-" for v in r]))


def load_field_sources(conn, run_id):
    cur = conn.cursor()
    cur.execute("SELECT ticker, field_sources FROM fundamental_scores WHERE run_id = %s", (run_id,))
    rows = cur.fetchall()
    cur.close()
    out = []
    for ticker, raw in rows:
        try:
            parsed = json.loads(raw) if raw else {}
        except (TypeError, json.JSONDecodeError):
            parsed = {}
        out.append((ticker, parsed))
    return out


def build_coverage_report(field_source_rows):
    """field: {source_type: count}, for every field that appears."""
    coverage = defaultdict(Counter)
    for ticker, sources in field_source_rows:
        for field, source in sources.items():
            if field == "_conflicts":
                continue
            coverage[field][source] += 1
    return coverage


def build_conflict_report(field_source_rows):
    """Returns (per_field_conflict_counts, per_ticker_conflicts,
    all_conflicts_flat) for the conflicts each ticker's row recorded."""
    per_field_counts = Counter()
    per_ticker_conflicts = defaultdict(list)
    all_conflicts = []
    for ticker, sources in field_source_rows:
        conflicts = sources.get("_conflicts", [])
        for c in conflicts:
            per_field_counts[c["field"]] += 1
            per_ticker_conflicts[ticker].append(c)
            all_conflicts.append((ticker, c))
    return per_field_counts, per_ticker_conflicts, all_conflicts


def print_coverage_section(coverage, total_tickers):
    print("\n=== Field coverage: where each indicator's raw value actually came from ===")
    rows = []
    for field in sorted(coverage.keys()):
        counts = coverage[field]
        edgar_n = counts.get("edgar", 0) + counts.get("edgar-derived", 0)
        yfinance_n = counts.get("yfinance", 0) + counts.get("derived", 0)  # 'derived' here means market-cap-based derivations, effectively yfinance-anchored
        proxy_n = sum(counts.get(s, 0) for s in PROXY_SOURCES)
        missing_n = sum(counts.get(s, 0) for s in MISSING_SOURCES)
        rows.append((
            field,
            f"{edgar_n} ({edgar_n*100//total_tickers}%)",
            f"{yfinance_n} ({yfinance_n*100//total_tickers}%)",
            f"{proxy_n} ({proxy_n*100//total_tickers}%)" if proxy_n else "-",
            f"{missing_n} ({missing_n*100//total_tickers}%)" if missing_n else "-",
        ))
    _print_table(rows, ["Field", "EDGAR", "yfinance", "Proxy/approx", "Missing"])
    print("\n  Fields with a high 'Proxy/approx' or 'Missing' percentage are the ones")
    print("  most worth fixing first (e.g. FCF Yield's OCF-proxy, Revenue Growth YoY")
    print("  never actually using EDGAR — see fetch_fundamental chat history for both).")


def print_conflict_section(per_field_counts, all_conflicts, top_n):
    print(f"\n=== EDGAR vs yfinance conflicts (>{20}% disagreement on the same field) ===")
    if not per_field_counts:
        print("  No conflicts detected in this run.")
        return
    rows = [(field, n) for field, n in per_field_counts.most_common()]
    print("\nBy field (how often EDGAR and yfinance disagreed):")
    _print_table(rows, ["Field", "Conflict count"])

    print(f"\nWorst individual conflicts (top {top_n} by % difference):")
    sorted_conflicts = sorted(all_conflicts, key=lambda tc: tc[1]["pct_diff"], reverse=True)[:top_n]
    rows2 = [
        (ticker, c["field"], c["edgar"], c["yfinance"], f"{c['pct_diff']}%")
        for ticker, c in sorted_conflicts
    ]
    _print_table(rows2, ["Ticker", "Field", "EDGAR value", "yfinance value", "% diff"])


def print_worst_tickers_section(per_ticker_conflicts, coverage_by_ticker, top_n):
    print(f"\n=== Tickers worth spot-checking by hand (top {top_n}) ===")
    # Rank by: number of conflicts, then number of missing/proxy fields
    scored = []
    for ticker, issues in coverage_by_ticker.items():
        n_conflicts = len(per_ticker_conflicts.get(ticker, []))
        n_bad = issues["missing"] + issues["proxy"]
        if n_conflicts == 0 and n_bad == 0:
            continue
        scored.append((ticker, n_conflicts, n_bad))
    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
    rows = scored[:top_n]
    _print_table(rows, ["Ticker", "# Conflicts", "# Missing/Proxy fields"])


def build_per_ticker_issue_counts(field_source_rows):
    out = {}
    for ticker, sources in field_source_rows:
        missing_n = sum(1 for f, s in sources.items() if f != "_conflicts" and s in MISSING_SOURCES)
        proxy_n = sum(1 for f, s in sources.items() if f != "_conflicts" and s in PROXY_SOURCES)
        out[ticker] = {"missing": missing_n, "proxy": proxy_n}
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--run-id", required=True, help="fundamental_scores.run_id to analyze")
    parser.add_argument("--top", type=int, default=TOP_N)
    args = parser.parse_args()

    conn = get_connection(args)
    field_source_rows = load_field_sources(conn, args.run_id)
    conn.close()

    if not field_source_rows:
        print(f"No fundamental_scores rows found for run_id={args.run_id!r}. Check the run_id.")
        return

    total = len(field_source_rows)
    print(f"=== Data quality report: run_id={args.run_id} ({total} tickers) ===")

    coverage = build_coverage_report(field_source_rows)
    print_coverage_section(coverage, total)

    per_field_conflicts, per_ticker_conflicts, all_conflicts = build_conflict_report(field_source_rows)
    print_conflict_section(per_field_conflicts, all_conflicts, args.top)

    issue_counts = build_per_ticker_issue_counts(field_source_rows)
    print_worst_tickers_section(per_ticker_conflicts, issue_counts, args.top)


if __name__ == "__main__":
    main()
