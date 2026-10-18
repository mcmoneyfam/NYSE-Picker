"""
show_results.py — view technical / fundamental / combined rankings for a
run without writing SQL by hand.

Default top-N is set by TOP_N below — change that one line, or override
per-call with --top, no need to touch anything else in this file.

Usage:
    python show_results.py --technical-run-id technical-LONG-DAILY-2026-08-22-7d720b
    python show_results.py --technical-run-id <id> --top 50
    python show_results.py --technical-run-id <id> --view technical
    python show_results.py --technical-run-id <id> --view fundamental
"""

import argparse
import os

import mysql.connector

# Change this one line to adjust the default for every run — no need to
# dig through the rest of the file.
TOP_N = 20


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
        print("  (no rows)")
        return
    widths = [max(len(str(h)), max(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * w for w in widths]))
    for r in rows:
        print(fmt.format(*[str(v) if v is not None else "-" for v in r]))


def resolve_fundamental_run_id(conn, technical_run_id, fundamental_run_id):
    if fundamental_run_id:
        return fundamental_run_id
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id FROM fundamental_scores WHERE technical_run_id = %s ORDER BY computed_at DESC LIMIT 1",
        (technical_run_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def show_technical(conn, run_id, top_n):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ticker, final_score, direction, timeframe
        FROM technical_scores WHERE run_id = %s
        ORDER BY final_score DESC LIMIT %s
        """,
        (run_id, top_n),
    )
    rows = cur.fetchall()
    cur.close()
    print(f"\n=== Top {top_n} — Technical (run_id={run_id}) ===")
    _print_table(rows, ["Ticker", "Technical Score", "Direction", "Timeframe"])


def show_fundamental(conn, run_id, top_n):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ticker, fundamental_score, quality_gate_applied
        FROM fundamental_scores WHERE run_id = %s
        ORDER BY fundamental_score DESC LIMIT %s
        """,
        (run_id, top_n),
    )
    rows = cur.fetchall()
    cur.close()
    print(f"\n=== Top {top_n} — Fundamental (run_id={run_id}) ===")
    _print_table(rows, ["Ticker", "Fundamental Score", "Quality Gate Applied"])


def show_combined(conn, technical_run_id, fundamental_run_id, top_n):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.ticker, t.final_score, f.fundamental_score,
               ROUND((t.final_score + f.fundamental_score) / 2, 2) AS combined_score
        FROM technical_scores t
        JOIN fundamental_scores f
            ON f.technical_run_id = t.run_id AND f.ticker = t.ticker
        WHERE t.run_id = %s AND f.run_id = %s
        ORDER BY combined_score DESC
        LIMIT %s
        """,
        (technical_run_id, fundamental_run_id, top_n),
    )
    rows = cur.fetchall()
    cur.close()
    print(f"\n=== Top {top_n} — Combined (technical={technical_run_id}, fundamental={fundamental_run_id}) ===")
    print("  NOTE: combined_score is a plain average, a quick look — not an official")
    print("  weighting. Phase 7 (composite scoring) is where a real weighting belongs.")
    _print_table(rows, ["Ticker", "Technical", "Fundamental", "Combined (avg)"])


def show_summary(conn, technical_run_id, fundamental_run_id):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM technical_scores WHERE run_id = %s", (technical_run_id,))
    tech_n = cur.fetchone()[0]
    fund_n = 0
    if fundamental_run_id:
        cur.execute("SELECT COUNT(*) FROM fundamental_scores WHERE run_id = %s", (fundamental_run_id,))
        fund_n = cur.fetchone()[0]
    cur.execute(
        "SELECT flag_type, COUNT(*) FROM data_flags "
        "WHERE ticker IN (SELECT ticker FROM technical_scores WHERE run_id = %s) "
        "GROUP BY flag_type",
        (technical_run_id,),
    )
    flag_rows = cur.fetchall()
    cur.close()
    print(f"\n=== Run summary ===")
    print(f"  Technical scored:   {tech_n}")
    print(f"  Fundamental scored: {fund_n}")
    if flag_rows:
        print("  Flags (across both phases, by ticker overlap with this technical run):")
        for flag_type, n in flag_rows:
            print(f"    {flag_type}: {n}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--technical-run-id", required=True)
    parser.add_argument("--fundamental-run-id", default=None, help="Defaults to the latest fundamental run linked to the given technical run")
    parser.add_argument("--top", type=int, default=TOP_N)
    parser.add_argument("--view", choices=["all", "technical", "fundamental", "combined"], default="all")
    args = parser.parse_args()

    conn = get_connection(args)
    fundamental_run_id = resolve_fundamental_run_id(conn, args.technical_run_id, args.fundamental_run_id)

    if args.view in ("all", "technical"):
        show_technical(conn, args.technical_run_id, args.top)
    if args.view in ("all", "fundamental"):
        if fundamental_run_id:
            show_fundamental(conn, fundamental_run_id, args.top)
        else:
            print("\n(no fundamental run linked to this technical run yet)")
    if args.view in ("all", "combined"):
        if fundamental_run_id:
            show_combined(conn, args.technical_run_id, fundamental_run_id, args.top)
        else:
            print("\n(no fundamental run linked — cannot show combined view)")

    show_summary(conn, args.technical_run_id, fundamental_run_id)
    conn.close()


if __name__ == "__main__":
    main()
