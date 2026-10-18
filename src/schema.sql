-- Stock Screener MVP — Phase 0 Schema (MySQL 8.0)
-- Design principles from brief:
--   - full data lineage: every score traceable to raw inputs
--   - completeness/staleness flags, no silent estimation
--   - tiering (Tier 1 = cheap technical+fundamental on full universe,
--     Tier 2 = expensive sentiment/ownership/NLP on shortlist+whitelist)
--   - always-include whitelist for mega-caps regardless of filter outcome
-- Live-tested in a real MySQL 8.0 instance: table creation, FK constraints,
-- upsert (ON DUPLICATE KEY UPDATE), and inserts across all six tables.

CREATE TABLE IF NOT EXISTS universe (
    ticker              VARCHAR(20) PRIMARY KEY,
    company_name        VARCHAR(255),
    exchange            VARCHAR(20) NOT NULL,
    security_type       VARCHAR(50),
    is_etf              BOOLEAN DEFAULT FALSE,
    is_test_issue       BOOLEAN DEFAULT FALSE,
    market_cap          DECIMAL(20,2),
    market_cap_asof     DATE,
    sector              VARCHAR(100),
    industry            VARCHAR(150),
    tier                TINYINT DEFAULT 1,
    is_whitelisted      BOOLEAN DEFAULT FALSE,
    is_active           BOOLEAN DEFAULT TRUE,
    first_seen          DATE NOT NULL,
    last_seen           DATE NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_universe_exchange (exchange),
    INDEX idx_universe_tier (tier),
    INDEX idx_universe_whitelist (is_whitelisted),
    INDEX idx_universe_sector (sector),
    INDEX idx_universe_marketcap (market_cap)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS whitelist_rules (
    rule_id             INT AUTO_INCREMENT PRIMARY KEY,
    rule_type           ENUM('market_cap_threshold', 'explicit_ticker', 'index_membership') NOT NULL,
    rule_value          VARCHAR(255) NOT NULL,
    description         TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ticker_aliases (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    nasdaq_ticker       VARCHAR(20) NOT NULL,
    yahoo_ticker        VARCHAR(50) NOT NULL,
    normalized_key      VARCHAR(50) NOT NULL,
    alias_type          ENUM('canonical', 'variant') DEFAULT 'variant',
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ticker_alias (nasdaq_ticker),
    INDEX idx_alias_yahoo (yahoo_ticker),
    INDEX idx_alias_key (normalized_key),
    CONSTRAINT fk_alias_ticker FOREIGN KEY (nasdaq_ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raw_metrics (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker              VARCHAR(20) NOT NULL,
    domain              ENUM('technical', 'fundamental', 'ownership', 'social_sentiment', 'news_sentiment', 'macro') NOT NULL,
    metric_name         VARCHAR(100) NOT NULL,
    metric_value        DOUBLE,
    metric_value_text   TEXT,
    source              VARCHAR(100) NOT NULL,
    as_of_date          DATE NOT NULL,
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_stale            BOOLEAN DEFAULT FALSE,
    UNIQUE KEY uq_raw_metric (ticker, domain, metric_name, source, as_of_date),
    INDEX idx_raw_ticker_domain (ticker, domain),
    CONSTRAINT fk_raw_ticker FOREIGN KEY (ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS data_flags (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker              VARCHAR(20) NOT NULL,
    domain              VARCHAR(50) NOT NULL,
    flag_type           ENUM('missing', 'stale', 'partial', 'source_error', 'rate_limited') NOT NULL,
    detail              TEXT,
    flagged_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_flags_ticker (ticker),
    CONSTRAINT fk_flags_ticker FOREIGN KEY (ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS scores (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker                  VARCHAR(20) NOT NULL,
    run_id                  VARCHAR(50) NOT NULL,
    timeframe_profile       ENUM('short', 'medium', 'long') NOT NULL,
    composite_score         DOUBLE,
    technical_score         DOUBLE,
    fundamental_score       DOUBLE,
    ownership_score         DOUBLE,
    social_sentiment_score  DOUBLE,
    news_sentiment_score    DOUBLE,
    sentiment_divergence    DOUBLE,
    macro_regime_flag       VARCHAR(50),
    data_completeness_pct   DOUBLE,
    last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_scores_ticker_run (ticker, run_id),
    INDEX idx_scores_run (run_id),
    INDEX idx_scores_ticker (ticker),
    CONSTRAINT fk_scores_ticker FOREIGN KEY (ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

-- Direction-aware, per-timeframe output of the Phase 1 scoring engine
-- (spec section 9). Separate from `scores.technical_score` above, which
-- is the single number that eventually feeds the Phase 7 composite —
-- this table holds the full LONG/SHORT x DAILY/WEEKLY/MONTHLY detail
-- with category breakdown, for drill-down and comparison.
CREATE TABLE IF NOT EXISTS technical_scores (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker                      VARCHAR(20) NOT NULL,
    run_id                      VARCHAR(50) NOT NULL,
    direction                   ENUM('LONG', 'SHORT') NOT NULL,
    timeframe                   ENUM('DAILY', 'WEEKLY', 'MONTHLY') NOT NULL,
    sector                      VARCHAR(100),
    industry                    VARCHAR(150),
    market_cap                  DECIMAL(20,2),
    final_score                 DOUBLE,
    overall_score_pre_atr       DOUBLE,
    volume_score                DOUBLE,
    momentum_score              DOUBLE,
    support_resistance_score    DOUBLE,
    ob_os_score                 DOUBLE,
    atr_multiplier_applied      DOUBLE,
    computed_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tech_scores (ticker, run_id, direction, timeframe),
    INDEX idx_tech_scores_run (run_id),
    INDEX idx_tech_scores_ticker (ticker),
    CONSTRAINT fk_tech_scores_ticker FOREIGN KEY (ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

-- Phase 2 — fundamental scoring output. Direction is not chosen
-- independently here: fetch_fundamental.py auto-links it from the
-- Phase 1 `technical_run_id` it's pointed at (see that script's
-- docstring), and `technical_run_id` records which run supplied it, for
-- lineage. `field_sources` is a JSON map (indicator -> 'edgar' /
-- 'yfinance' / 'derived' / 'missing') so every score stays traceable to
-- its raw inputs per spec section 1.
CREATE TABLE IF NOT EXISTS fundamental_scores (
    id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker                   VARCHAR(20) NOT NULL,
    run_id                   VARCHAR(50) NOT NULL,
    technical_run_id         VARCHAR(50),
    direction                ENUM('LONG', 'SHORT') NOT NULL,
    fundamental_score        DOUBLE,
    pe_ratio_score           DOUBLE,
    ev_ebitda_score          DOUBLE,
    fcf_yield_score          DOUBLE,
    roe_score                DOUBLE,
    gross_margin_score       DOUBLE,
    ocf_ni_score             DOUBLE,
    revenue_growth_score     DOUBLE,
    net_debt_ebitda_score    DOUBLE,
    short_percent_float_score DOUBLE,
    buyback_yield_score      DOUBLE,
    roa_score                DOUBLE,
    quality_gate_applied     BOOLEAN DEFAULT FALSE,
    field_sources            JSON,
    computed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_fund_scores (ticker, run_id),
    INDEX idx_fund_scores_run (run_id),
    INDEX idx_fund_scores_ticker (ticker),
    CONSTRAINT fk_fund_scores_ticker FOREIGN KEY (ticker) REFERENCES universe(ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              VARCHAR(50) PRIMARY KEY,
    started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMP NULL,
    universe_size       INT,
    tier2_size          INT,
    status              ENUM('running', 'completed', 'failed') DEFAULT 'running',
    notes               TEXT
) ENGINE=InnoDB;

