-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Instruments table
CREATE TABLE IF NOT EXISTS instruments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN ('stock', 'etf', 'commodity')),
    sector VARCHAR(50) CHECK (sector IN (
        'technology', 'financials', 'healthcare', 'consumer_discretionary',
        'consumer_staples', 'communication', 'energy', 'industrials',
        'materials', 'utilities', 'real_estate'
    )),
    yfinance_symbol VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Historical OHLCV prices
CREATE TABLE IF NOT EXISTS historical_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open NUMERIC(20, 6) NOT NULL,
    high NUMERIC(20, 6) NOT NULL,
    low NUMERIC(20, 6) NOT NULL,
    close NUMERIC(20, 6) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, date)
);

-- Live prices
CREATE TABLE IF NOT EXISTS live_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    price NUMERIC(20, 6) NOT NULL,
    change_amount NUMERIC(20, 6),
    change_percent NUMERIC(10, 4),
    market_status VARCHAR(20) NOT NULL DEFAULT 'closed',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- News articles
CREATE TABLE IF NOT EXISTS news_articles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title TEXT NOT NULL,
    link TEXT,
    summary TEXT,
    content TEXT,
    source VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN (
        'macro_markets', 'macro_politics', 'macro_conflict', 'asset_specific',
        'sector_technology', 'sector_financials', 'sector_healthcare',
        'sector_consumer_discretionary', 'sector_consumer_staples',
        'sector_communication', 'sector_energy', 'sector_industrials',
        'sector_materials', 'sector_utilities', 'sector_real_estate'
    )),
    is_macro BOOLEAN NOT NULL DEFAULT false,
    is_asset_specific BOOLEAN NOT NULL DEFAULT false,
    ollama_processed BOOLEAN NOT NULL DEFAULT false,
    macro_sentiment_label VARCHAR(30),
    macro_long_term_label VARCHAR(30),
    published_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (title, source)
);

-- Map news articles to instruments (for instrument-specific financial news)
CREATE TABLE IF NOT EXISTS news_instrument_map (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    relevance_score NUMERIC(5, 4) DEFAULT 0,
    UNIQUE (article_id, instrument_id)
);

-- Sentiment scores — dual short-term / long-term from LLM
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    positive NUMERIC(7, 6) NOT NULL,
    negative NUMERIC(7, 6) NOT NULL,
    neutral NUMERIC(7, 6) NOT NULL,
    label VARCHAR(30) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral', 'very positive', 'very negative')),
    long_term_label VARCHAR(30) CHECK (long_term_label IN ('positive', 'negative', 'neutral', 'very positive', 'very negative')),
    long_term_confidence NUMERIC(7, 6) DEFAULT 0.5,
    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (article_id)
);

-- Technical analysis indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    indicator_name VARCHAR(50) NOT NULL,
    value JSONB NOT NULL,
    signal VARCHAR(20) CHECK (signal IN ('strong_buy', 'buy', 'neutral', 'sell', 'strong_sell')),
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, date, indicator_name)
);

-- Investment grades (NUMERIC(7,4) preserves full precision from scorer)
CREATE TABLE IF NOT EXISTS grades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    term VARCHAR(10) NOT NULL CHECK (term IN ('short', 'long')),
    overall_grade VARCHAR(20) NOT NULL,
    overall_score NUMERIC(7, 4) NOT NULL,
    technical_score NUMERIC(7, 4) NOT NULL,
    sentiment_score NUMERIC(7, 4) NOT NULL,
    macro_score NUMERIC(7, 4) NOT NULL,
    sector_score NUMERIC(7, 4) NOT NULL DEFAULT 0,
    fundamentals_score NUMERIC(7, 4) NOT NULL DEFAULT 0,
    details JSONB,
    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Macro sentiment (rolling, latest only) — term-aware (short/long)
CREATE TABLE IF NOT EXISTS macro_sentiment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region VARCHAR(10) NOT NULL DEFAULT 'global',
    term VARCHAR(10) NOT NULL DEFAULT 'short' CHECK (term IN ('short', 'long')),
    score NUMERIC(7, 6) NOT NULL,
    label VARCHAR(10) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral')),
    article_count INT NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sector sentiment (rolling, term-aware) — one per sector per term
CREATE TABLE IF NOT EXISTS sector_sentiment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sector VARCHAR(50) NOT NULL CHECK (sector IN (
        'technology', 'financials', 'healthcare', 'consumer_discretionary',
        'consumer_staples', 'communication', 'energy', 'industrials',
        'materials', 'utilities', 'real_estate'
    )),
    term VARCHAR(10) NOT NULL DEFAULT 'short' CHECK (term IN ('short', 'long')),
    score NUMERIC(7, 6) NOT NULL,
    label VARCHAR(10) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral')),
    article_count INT NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Intraday prices (5-minute candles for 1D chart)
CREATE TABLE IF NOT EXISTS intraday_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 6) NOT NULL,
    high NUMERIC(20, 6) NOT NULL,
    low NUMERIC(20, 6) NOT NULL,
    close NUMERIC(20, 6) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    UNIQUE (instrument_id, timestamp)
);

-- Portfolio (user watchlist)
CREATE TABLE IF NOT EXISTS portfolio (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(instrument_id)
);

-- ETF constituent weights — maps ETFs to their underlying holdings with % weights
CREATE TABLE IF NOT EXISTS etf_constituents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    etf_instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    constituent_symbol VARCHAR(20) NOT NULL,
    constituent_name VARCHAR(255) NOT NULL,
    weight_percent NUMERIC(7, 4) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(etf_instrument_id, constituent_symbol)
);

-- Fundamental metrics (P/E, ROE, D/E, PEG) — fetched daily from FMP
CREATE TABLE IF NOT EXISTS fundamental_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    pe_ratio NUMERIC(12, 4),
    roe NUMERIC(12, 6),
    de_ratio NUMERIC(12, 4),
    peg_ratio NUMERIC(12, 4),
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Macro economic indicators (DXY, 10Y Treasury, GDP Growth, Brent Crude) — fetched daily from FRED
CREATE TABLE IF NOT EXISTS macro_indicators (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    indicator_name VARCHAR(50) NOT NULL,
    value NUMERIC(16, 6) NOT NULL,
    label VARCHAR(100) NOT NULL,
    unit VARCHAR(20) NOT NULL DEFAULT '',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Processing priority (user-triggered, signals processor to prioritize an instrument's news)
CREATE TABLE IF NOT EXISTS processing_priority (
    instrument_id UUID PRIMARY KEY REFERENCES instruments(id) ON DELETE CASCADE,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_historical_prices_instrument_date ON historical_prices(instrument_id, date DESC);
CREATE INDEX idx_live_prices_instrument_fetched ON live_prices(instrument_id, fetched_at DESC);
CREATE INDEX idx_news_articles_category_published ON news_articles(category, published_at DESC);
CREATE INDEX idx_news_articles_fetched ON news_articles(fetched_at DESC);
CREATE INDEX idx_news_articles_unprocessed ON news_articles(ollama_processed, fetched_at DESC) WHERE ollama_processed = false;
CREATE INDEX idx_sentiment_scores_article ON sentiment_scores(article_id);
CREATE INDEX idx_technical_indicators_instrument_date ON technical_indicators(instrument_id, date DESC);
CREATE INDEX idx_grades_instrument_term ON grades(instrument_id, term, graded_at DESC);
CREATE INDEX idx_macro_sentiment_region_calc ON macro_sentiment(region, term, calculated_at DESC);
CREATE INDEX idx_etf_constituents_etf ON etf_constituents(etf_instrument_id);
CREATE INDEX idx_news_instrument_map_instrument ON news_instrument_map(instrument_id);
CREATE INDEX idx_intraday_prices_instrument_ts ON intraday_prices(instrument_id, timestamp DESC);
CREATE INDEX idx_sector_sentiment_sector_term ON sector_sentiment(sector, term, calculated_at DESC);
CREATE INDEX idx_instruments_sector ON instruments(sector) WHERE sector IS NOT NULL;
CREATE INDEX idx_fundamental_metrics_instrument ON fundamental_metrics(instrument_id, fetched_at DESC);
CREATE INDEX idx_macro_indicators_name_fetched ON macro_indicators(indicator_name, fetched_at DESC);

-- Seed instruments
INSERT INTO instruments (symbol, name, category, sector, yfinance_symbol) VALUES
    -- Stocks
    ('RTX', 'RTX Corporation', 'stock', 'industrials', 'RTX'),
    ('NVDA', 'NVIDIA Corporation', 'stock', 'technology', 'NVDA'),
    ('GOOGL', 'Alphabet Inc.', 'stock', 'communication', 'GOOGL'),
    ('AAPL', 'Apple Inc.', 'stock', 'technology', 'AAPL'),
    ('TSLA', 'Tesla, Inc.', 'stock', 'consumer_discretionary', 'TSLA'),
    ('PLTR', 'Palantir Technologies Inc.', 'stock', 'technology', 'PLTR'),
    ('LLY', 'Eli Lilly and Company', 'stock', 'healthcare', 'LLY'),
    ('NVO', 'Novo Nordisk A/S', 'stock', 'healthcare', 'NVO'),
    ('WMT', 'Walmart Inc.', 'stock', 'consumer_staples', 'WMT'),
    ('XOM', 'Exxon Mobil Corporation', 'stock', 'energy', 'XOM'),
    -- ETFs
    ('IITU', 'iShares US Technology ETF', 'etf', 'technology', 'IITU.L'),
    ('SMH', 'VanEck Semiconductor ETF', 'etf', 'technology', 'SMH'),
    ('VOO', 'Vanguard S&P 500 ETF', 'etf', NULL, 'VOO'),
    -- Commodities
    ('GOLD', 'Gold Futures', 'commodity', 'materials', 'GC=F'),
    ('OIL', 'Crude Oil Futures', 'commodity', 'energy', 'CL=F')
ON CONFLICT (symbol) DO NOTHING;
