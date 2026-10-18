# NYSE-Picker — Phase 0

Scaffold, MySQL schema, and universe fetch.

## Setup

```bash
pip install -r requirements.txt

# create the database once, e.g.:
mysql -u root -p -e "CREATE DATABASE screener;"

export MYSQL_HOST=localhost MYSQL_DB=screener MYSQL_USER=<you> MYSQL_PASSWORD=<...>
cd src
python fetch_universe.py
# or: python fetch_universe.py --host localhost --db screener --user root --password ...
```

This must run on a machine with normal internet access (Nasdaq Trader's
endpoints aren't reachable from this sandbox). The schema and the
database-side logic in `fetch_universe.py` (table creation, FK constraints,
the upsert path, whitelist application, tier assignment, idempotency on
re-runs) were all live-tested against a real MySQL 8.0 instance in this
sandbox — only the actual network fetch of the symbol directories is
untested, since that domain isn't reachable here. Run it locally and
confirm counts look right (roughly 3,000-3,500 NASDAQ symbols,
~2,500-3,000 NYSE/AMEX as of recent listings — flag me if actual counts are
far off, could mean a parsing issue).

## What's here

- `src/schema.sql` — MySQL 8.0 DDL, six tables: `universe`, `whitelist_rules`,
  `raw_metrics`, `data_flags`, `scores`, `pipeline_runs`. Live-tested:
  table creation, foreign keys, inserts all ran clean.
- `src/fetch_universe.py` — pulls NASDAQ + NYSE/AMEX symbol directories,
  upserts into `universe` via `ON DUPLICATE KEY UPDATE`, marks delisted
  tickers inactive (never deletes, for history), applies explicit-ticker
  whitelist rules. Uses `mysql-connector-python`; connects via CLI args or
  standard `MYSQL_*` env vars.

## Phase 1 — Technical layer (v2, spec-driven)

Rebuilt against a formal spec: direction-aware (LONG/SHORT, separate
formulas, no inversion), timeframe-aware (DAILY/WEEKLY/MONTHLY, with
proper resampling), config-driven (all periods/thresholds/weights in
`config.yaml`, nothing hard-coded), four-category structure (Volume 40%,
Momentum 25%, Support/Resistance 20%, Overbought/Oversold 15%) with an
ATR-based risk multiplier applied last. Supersedes the earlier
RSI/MACD/RVOL/beta-blend version.

- `config.yaml` — every indicator period, OB/OS threshold, category
  weight, sub-weight, and ATR multiplier, per timeframe. Edit this to
  retune scoring; no code changes needed.
- `src/indicators.py` — raw indicator math (RVOL, vol-vs-MA, MFI, MACD
  histogram, ADX, RSI, Bollinger %B, ATR, 52-bar range position), plus
  OHLCV resampling for WEEKLY/MONTHLY. Pure functions, no DB/network
  dependency.
- `src/scoring.py` — direction-specific scoring formulas for all four
  categories, category aggregation, and the ATR risk-adjustment step.
  Also pure functions.
- `src/fetch_technical.py` — orchestrator: pulls OHLCV via yfinance,
  runs the above, writes raw indicators to `raw_metrics` and full
  results (final score + category breakdown) to `technical_scores`.
  Computes exactly one direction/timeframe combination per run, per the
  spec's "no simultaneous scoring" requirement — call it multiple times
  to cover more combinations. Per-ticker failures are logged to
  `data_flags` and skipped rather than aborting the run.

**What's been tested live, and what hasn't:**
- `indicators.py` + `scoring.py`: live-tested against synthetic OHLCV
  data across all three timeframes and both directions — bounds checks
  (all scores 0-100), LONG vs. SHORT produce genuinely different (not
  mirrored) results, ATR floor/ceiling multipliers trigger correctly,
  and missing/NaN indicator values renormalize instead of crashing.
- `fetch_technical.py`'s database logic: live-tested against real MySQL
  with mocked scoring output — inserts, FK constraints, and the
  idempotent upsert on `(ticker, run_id, direction, timeframe)` all
  confirmed working, including that different direction/timeframe
  combos correctly produce separate rows rather than overwriting.
- **Not yet tested**: the actual yfinance network fetch, since this
  sandbox has no route to Yahoo's API. Run a small `--limit 10` smoke
  test locally first.

```bash
python fetch_technical.py --direction LONG --timeframe DAILY --limit 10   # smoke test
python fetch_technical.py --direction LONG --timeframe DAILY --run-id 2026-08-15-long-daily
python fetch_technical.py --direction SHORT --timeframe WEEKLY --run-id 2026-08-15-short-weekly
```

**Known deviations / judgment calls** (spec section 11 allows this, with
documentation):
- Weighted category aggregation renormalizes across available
  sub-indicators when one is NaN, rather than treating it as zero — the
  spec doesn't cover missing-data behavior explicitly, and zeroing out
  would unfairly tank a category over one bad input.
- `52-week` range position uses a rolling 52-bar window on whatever
  timeframe is active (52 daily bars ≠ 52 weeks) — this matches spec
  section 10's own clarification, just flagging it since the naming is
  easy to misread.

## Sector / market cap filtering

- `src/fetch_sector_marketcap.py` — one-time (or periodic) enrichment
  step: populates `universe.sector`, `.industry`, `.market_cap` via
  yfinance's `.info` lookup. Run this after `fetch_universe.py`, before
  using `--sector`/`--min-market-cap`/`--max-market-cap` below (only
  fetches tickers missing this data by default; `--refresh-stale-days N`
  re-fetches anything older than N days too).
- `fetch_technical.py` now accepts `--sector`, `--min-market-cap`,
  `--max-market-cap` to filter which tickers get scored. Sector must
  match `universe.sector` exactly (yfinance's own sector labels, e.g.
  `Technology`, `Healthcare`, `Financial Services` — check what's
  actually in your data with `SELECT DISTINCT sector FROM universe;`).
- `technical_scores` now stores a `sector`/`industry`/`market_cap`
  snapshot alongside each score row (denormalized from `universe` at
  scoring time), so results are readable/filterable without a join.
- Live-tested against real MySQL: sector filter, market-cap range
  filter, combined filter, and the denormalized snapshot write all
  confirmed working. The actual yfinance `.info` calls are untested
  here, same network limitation as everywhere else.

```bash
python fetch_sector_marketcap.py --limit 500        # enrich a batch first
python fetch_technical.py --direction LONG --timeframe DAILY --sector Technology
python fetch_technical.py --direction LONG --timeframe DAILY --min-market-cap 10000000000
```

**Important — NULL sector/market_cap does not mean the ticker was
removed.** It means "not enriched yet"; the ticker stays active in
`universe` and still scores normally in unfiltered runs. What changed
just now: if you run a *filtered* query (`--sector`/`--min-market-cap`/
`--max-market-cap`), unenriched tickers were previously silently
skipped — indistinguishable from tickers that were checked and
genuinely didn't match. `fetch_technical.py` now prints a warning with
a count whenever a filtered run has unenriched tickers being excluded,
so this is visible instead of silent (verified live against MySQL:
correctly counts 0 when no filter is active, correctly counts and
excludes unenriched tickers when a filter is on).

For display, use `COALESCE` at query time rather than writing a
placeholder into the table — keeps the DB's `NULL` honest (meaningfully
different from market cap actually being zero) while still showing
something readable:

```sql
SELECT ticker,
       COALESCE(sector, 'Not Identified') AS sector,
       COALESCE(CAST(market_cap AS CHAR), 'Not Identified') AS market_cap
FROM universe
WHERE is_active = TRUE
ORDER BY ticker;
```

## Known data-quality gap: non-equity symbols

The Nasdaq Trader symbol directory (used by `fetch_universe.py`) includes
SPAC units, warrants, and preferred shares alongside common stock — e.g.
`AAC.U` (unit), `ACHR.W` (warrant), `ABR$D` (preferred). Yahoo Finance
doesn't track most of these individually, so they show up as `missing`
in `data_flags` rather than scoring — harmless today, but worth filtering
out at the `universe` stage later (via `security_type`/name-pattern
matching) so the flagged-count noise doesn't obscure genuine data gaps.

## Why MySQL (over SQLite, over Postgres)

Moved off the original SQLite draft for the same reason Postgres was
considered: better concurrent read/write if a dashboard or scheduler ends
up querying live, and a better fit for `raw_metrics`'s key/value structure
at scale. Landed on MySQL specifically because it was the one I could
actually get running and verify live in this environment — Postgres's
install failed on a package-mirror issue that a retry likely would have
fixed too, so this wasn't a judgment that MySQL is technically better for
this project; it's the one with live-tested evidence behind it. Both
remain reasonable choices. Schema differences from the SQLite version:
`AUTO_INCREMENT` instead of `AUTOINCREMENT`, `ENUM` columns instead of
comments for enum-like fields, `ON UPDATE CURRENT_TIMESTAMP` for
`updated_at` (built into MySQL, no trigger needed unlike Postgres).

## Open decisions

1. **Whitelist definition** — `whitelist_rules` supports `explicit_ticker`,
   `market_cap_threshold`, and `index_membership` rule types, but only
   `explicit_ticker` is wired up yet. Market-cap-based whitelisting needs a
   market cap column populated first — that's a small follow-up script
   (`fetch_market_caps.py`, using yfinance) I'd fold into Phase 1 since
   Phase 1's technical layer already needs yfinance wired in.
2. **Whitelist threshold value** — brief says "mega-cap names" without a
   number. Common conventions: >$200B, or S&P 500 membership, or a fixed
   list (e.g. Magnificent Seven + a few others). Your call — I'd default to
   a market cap threshold since it's self-maintaining (no list to update),
   but happy to hardcode a list instead if you'd rather have explicit
   control over exactly which names always show up.
3. **`sentiment_divergence` calculation** — still undefined in the output
   spec. Options: `|social_score - news_score|` (magnitude only, ignores
   direction) vs. `social_score - news_score` (signed, shows which source
   is more bullish). I'd lean signed since direction seems like the more
   useful signal, but this is a scoring-phase decision (Phase 4/5), not
   blocking yet.
4. **`technical_score` weighting** — the RSI/MACD/RVOL/range/beta blend in
   `technical.py` is a first-pass heuristic (see Phase 1 section above),
   open to adjustment once there's a backtesting harness to validate against.

## Next: Phase 2

Fundamental layer (valuation/profitability/balance sheet/dividends/FCF) —
follows the same pattern as Phase 1: pure calculation functions tested
against synthetic data, then a yfinance/SEC-EDGAR orchestrator writing to
`raw_metrics` and `scores.fundamental_score`.
