"""
Phase 0 — Universe fetch (MySQL version).

Pulls the full US-listed equity universe (NASDAQ/NYSE/AMEX) from Nasdaq
Trader's free, public symbol directory files. See schema.sql for table
definitions.

Requires a running MySQL 8.0 instance. Connection is via env vars or --dsn
components:

    export MYSQL_HOST=localhost MYSQL_DB=screener MYSQL_USER=screener_user MYSQL_PASSWORD=...
    python fetch_universe.py

or:

    python fetch_universe.py --host localhost --db screener --user root --password ...

Run this on a machine with normal internet access — Nasdaq Trader's
endpoints are not reachable from this sandboxed dev environment, so this
script is a deliverable to run locally, not something executed here.
(The schema itself — table creation, FK constraints, upserts, inserts —
was live-tested against a real MySQL 8.0 instance in this sandbox.)
"""

import argparse
import csv
import io
import os
import re
from datetime import date

import mysql.connector
import requests

# Nasdaq Trader has moved this endpoint across subdomains before
# (old.nasdaqtrader.com -> www.nasdaqtrader.com). Each entry is tried in
# order until one works, so a future move doesn't silently break the script.
NASDAQ_URL_CANDIDATES = [
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "https://old.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
]
OTHER_URL_CANDIDATES = [
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    "https://old.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
]

# Some Nasdaq Trader endpoints reject requests with no browser-like
# User-Agent header.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; screener-fetch/1.0)"}

OTHER_EXCHANGE_MAP = {
    "A": "AMEX",
    "N": "NYSE",
}

TODAY = date.today().isoformat()


def _fetch_text(url: str) -> str:
    resp = requests.get(url, timeout=30, headers=_HEADERS)
    resp.raise_for_status()
    return resp.text


def _fetch_text_with_fallback(url_candidates: list[str]) -> str:
    """Tries each URL in order, raising the last error if all fail."""
    last_error = None
    for url in url_candidates:
        try:
            return _fetch_text(url)
        except requests.exceptions.RequestException as e:
            print(f"  ...failed to fetch {url} ({e.__class__.__name__}), trying next candidate")
            last_error = e
    raise last_error


def parse_nasdaqlisted(text: str):
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    for row in reader:
        symbol = row.get("Symbol", "").strip()
        if not symbol or symbol.startswith("File Creation Time"):
            continue
        yield (
            symbol,
            row.get("Security Name", "").strip(),
            "NASDAQ",
            row.get("ETF", "N") == "Y",
            row.get("Test Issue", "N") == "Y",
        )


def parse_otherlisted(text: str):
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    for row in reader:
        symbol = row.get("ACT Symbol", "").strip()
        if not symbol or symbol.startswith("File Creation Time"):
            continue
        exch_code = row.get("Exchange", "").strip()
        exchange = OTHER_EXCHANGE_MAP.get(exch_code)
        if exchange is None:
            continue  # skip Arca/BATS/IEX etc. — out of scope per brief
        yield (
            symbol,
            row.get("Security Name", "").strip(),
            exchange,
            row.get("ETF", "N") == "Y",
            row.get("Test Issue", "N") == "Y",
        )


# Keywords in Nasdaq Trader's "Security Name" field that indicate a
# non-common-share instrument. A suffix letter alone (e.g. the 'D' in
# 'ABR$D') can't distinguish "Class D common stock" from "Series D
# preferred stock" — both look identical as a ticker suffix — so this
# checks the security name text instead, which is where Nasdaq actually
# encodes the distinction (e.g. "Arbor Realty Trust Inc - Depositary
# Shares... Series D Preferred").
_NON_COMMON_SHARE_KEYWORDS = (
    "PREFERRED", "PFD", "DEPOSITARY", "DEPOSITORY", "WARRANT",
    "RIGHT", "RIGHTS", "UNIT", "UNITS", "NOTE", "DEBENTURE",
)


def _is_non_common_share(security_name):
    if not security_name:
        return False
    name = security_name.upper()
    return any(kw in name for kw in _NON_COMMON_SHARE_KEYWORDS)


def normalize_yahoo_ticker(ticker, security_name=None):
    """Canonicalize a Nasdaq/NYSE ticker into Yahoo's symbol form.

    Exact rules:
    - class-share variants use the same base symbol with '-' instead of '$' or '.'
    - common stock class suffixes A..Z are canonicalized (BRK.B -> BRK-B, C$R -> C-R)
    - units/warrants/preferreds and other non-common-share variants are intentionally rejected,
      using both the suffix letter (R/S/T/U/V/W/X/Y/Z, e.g. rights/units/warrants) and the
      Security Name text (e.g. 'Preferred'/'Depositary', which a suffix letter alone can't catch)
    """
    if ticker is None:
        return ticker
    value = str(ticker).strip().upper()
    if not value:
        return value
    if '-' in value:
        return value

    if re.search(r"(?:[$.][A-Z])$", value):
        suffix = value[-1]
        base = value[:-2]
        # Both '$' and '.' separators can carry non-common-share suffixes
        # (e.g. 'ABR$D' is a preferred share, per the Nasdaq Trader file —
        # see README's "Known data-quality gap" section), so apply the same
        # filter to both instead of letting '$' through unfiltered.
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ") - {"R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
        if suffix not in allowed:
            return None
        if _is_non_common_share(security_name):
            return None
        return f"{base}-{suffix}"

    return value


def upsert_universe(conn, rows):
    n = 0
    cur = conn.cursor()
    for ticker, name, exchange, is_etf, is_test in rows:
        cur.execute(
            """
            INSERT INTO universe
                (ticker, company_name, exchange, is_etf, is_test_issue,
                 first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                company_name  = VALUES(company_name),
                exchange      = VALUES(exchange),
                is_etf        = VALUES(is_etf),
                is_test_issue = VALUES(is_test_issue),
                is_active     = TRUE,
                last_seen     = VALUES(last_seen)
            """,
            (ticker, name, exchange, is_etf, is_test, TODAY, TODAY),
        )
        n += 1
    conn.commit()
    cur.close()
    return n


def upsert_ticker_aliases(conn, rows):
    cur = conn.cursor()
    n = 0
    for ticker, security_name, _, _, _ in rows:
        yahoo_ticker = normalize_yahoo_ticker(ticker, security_name)
        if not yahoo_ticker:
            continue
        cur.execute(
            """
            INSERT INTO ticker_aliases (nasdaq_ticker, yahoo_ticker, normalized_key, alias_type)
            VALUES (%s, %s, %s, 'canonical')
            ON DUPLICATE KEY UPDATE
                yahoo_ticker = VALUES(yahoo_ticker),
                normalized_key = VALUES(normalized_key),
                alias_type = 'canonical',
                updated_at = CURRENT_TIMESTAMP
            """,
            (ticker, yahoo_ticker, yahoo_ticker),
        )
        n += 1
    conn.commit()
    cur.close()
    return n


def mark_delisted(conn):
    cur = conn.cursor()
    cur.execute("UPDATE universe SET is_active = FALSE WHERE last_seen != %s", (TODAY,))
    count = cur.rowcount
    conn.commit()
    cur.close()
    return count


def apply_whitelist_rules(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT rule_value FROM whitelist_rules "
        "WHERE rule_type = 'explicit_ticker' AND is_active = TRUE"
    )
    tickers = [r[0] for r in cur.fetchall()]
    if tickers:
        cur.executemany(
            "UPDATE universe SET is_whitelisted = TRUE, tier = 2 WHERE ticker = %s",
            [(t,) for t in tickers],
        )
    conn.commit()
    cur.close()
    return len(tickers)


def get_connection(args):
    return mysql.connector.connect(
        host=args.host or os.environ.get("MYSQL_HOST", "localhost"),
        database=args.db or os.environ.get("MYSQL_DB", "screener"),
        user=args.user or os.environ.get("MYSQL_USER", "root"),
        password=args.password or os.environ.get("MYSQL_PASSWORD", ""),
        port=args.port or int(os.environ.get("MYSQL_PORT", "3306")),
    )


def apply_schema(conn, schema_path):
    """Runs schema.sql. CREATE TABLE IF NOT EXISTS handles brand-new
    tables fine, but does nothing for columns added to a table that
    already exists (e.g. sector/industry added to universe after your
    first run) — that's what run_migrations() below is for."""
    with open(schema_path) as f:
        cur = conn.cursor()
        for statement in f.read().split(";"):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        conn.commit()
        cur.close()


# Column additions made to schema.sql after the initial release. Each
# entry is (table, column_name, column_definition_sql). New columns
# should be appended here going forward, in addition to schema.sql,
# so existing databases self-heal on the next fetch_universe.py run
# instead of erroring with "Unknown column" the way sector/industry
# did before this migration step existed.
MIGRATIONS = [
    ("universe", "sector", "VARCHAR(100)"),
    ("universe", "industry", "VARCHAR(150)"),
    ("technical_scores", "sector", "VARCHAR(100)"),
    ("technical_scores", "industry", "VARCHAR(150)"),
    ("technical_scores", "market_cap", "DECIMAL(20,2)"),
    ("ticker_aliases", "nasdaq_ticker", "VARCHAR(20)"),
]


def run_migrations(conn):
    cur = conn.cursor()
    for table, column, definition in MIGRATIONS:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s
            """,
            (table, column),
        )
        exists = cur.fetchone()[0] > 0
        if not exists:
            print(f"  migrating: adding {table}.{column} ...")
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--schema", default="schema.sql")
    args = parser.parse_args()

    conn = get_connection(args)

    apply_schema(conn, args.schema)
    run_migrations(conn)

    print("Fetching NASDAQ listing...")
    nasdaq_rows = list(parse_nasdaqlisted(_fetch_text_with_fallback(NASDAQ_URL_CANDIDATES)))
    print(f"  {len(nasdaq_rows)} NASDAQ symbols")

    print("Fetching NYSE/AMEX listing...")
    other_rows = list(parse_otherlisted(_fetch_text_with_fallback(OTHER_URL_CANDIDATES)))
    print(f"  {len(other_rows)} NYSE/AMEX symbols")

    rows = nasdaq_rows + other_rows
    total = upsert_universe(conn, rows)
    alias_count = upsert_ticker_aliases(conn, rows)
    delisted = mark_delisted(conn)
    whitelisted = apply_whitelist_rules(conn)

    print(f"Upserted {total} tickers. Stored {alias_count} Nasdaq→Yahoo alias mappings.")
    print(f"Marked {delisted} inactive/delisted.")
    print(f"Applied explicit-ticker whitelist to {whitelisted} tickers.")
    print(
        "NOTE: market-cap-threshold whitelist rules are not yet applied — "
        "run fetch_market_caps.py (Phase 0 follow-up) before relying on "
        "cap-based whitelisting or Tier 2 assignment."
    )

    conn.close()


if __name__ == "__main__":
    main()
