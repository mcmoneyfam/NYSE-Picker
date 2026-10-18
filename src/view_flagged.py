"""View flagged tickers from the data_flags table."""

import argparse
import os
import mysql.connector


def get_connection(args):
    return mysql.connector.connect(
        host=args.host or os.environ.get("MYSQL_HOST", "localhost"),
        database=args.db or os.environ.get("MYSQL_DB", "screener"),
        user=args.user or os.environ.get("MYSQL_USER", "root"),
        password=args.password or os.environ.get("MYSQL_PASSWORD", ""),
        port=args.port or int(os.environ.get("MYSQL_PORT", "3306")),
    )


def main():
    parser = argparse.ArgumentParser(description="View flagged tickers")
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--domain", default="technical", help="Filter by domain (technical, fundamental, etc.)")
    parser.add_argument("--flag-type", default=None, help="Filter by flag type (missing, stale, partial, source_error)")
    parser.add_argument("--limit", type=int, default=100, help="Max results to show")
    args = parser.parse_args()

    conn = get_connection(args)
    cur = conn.cursor()

    query = "SELECT ticker, domain, flag_type, detail, flagged_at FROM data_flags WHERE domain = %s"
    params = [args.domain]

    if args.flag_type:
        query += " AND flag_type = %s"
        params.append(args.flag_type)

    query += f" ORDER BY flagged_at DESC LIMIT {int(args.limit)}"

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print(f"No flagged tickers found (domain={args.domain})")
        return

    print(f"\n{'TICKER':<15} {'FLAG_TYPE':<15} {'DETAIL':<60}")
    print("-" * 90)
    for ticker, domain, flag_type, detail, flagged_at in rows:
        detail_short = (detail[:57] + "...") if detail and len(detail) > 60 else (detail or "")
        print(f"{ticker:<15} {flag_type:<15} {detail_short:<60}")

    print(f"\nTotal: {len(rows)} flagged tickers")


if __name__ == "__main__":
    main()
