-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Instruments table
CREATE TABLE IF NOT EXISTS instruments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN ('stock', 'etf', 'commodity')),
    yfinance_symbol VARCHAR(20) NOT NULL,
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
    source VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN ('us_politics', 'uk_politics', 'us_finance', 'uk_finance')),
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

-- Sentiment scores (from FinBERT)
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    positive NUMERIC(7, 6) NOT NULL,
    negative NUMERIC(7, 6) NOT NULL,
    neutral NUMERIC(7, 6) NOT NULL,
    label VARCHAR(10) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral')),
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

-- Investment grades
CREATE TABLE IF NOT EXISTS grades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    term VARCHAR(10) NOT NULL CHECK (term IN ('short', 'long')),
    overall_grade VARCHAR(2) NOT NULL,
    overall_score NUMERIC(5, 2) NOT NULL,
    technical_score NUMERIC(5, 2) NOT NULL,
    sentiment_score NUMERIC(5, 2) NOT NULL,
    macro_score NUMERIC(5, 2) NOT NULL,
    details JSONB,
    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Macro sentiment (rolling, latest only)
CREATE TABLE IF NOT EXISTS macro_sentiment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region VARCHAR(10) NOT NULL CHECK (region IN ('us', 'uk')),
    score NUMERIC(7, 6) NOT NULL,
    label VARCHAR(10) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral')),
    article_count INT NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_historical_prices_instrument_date ON historical_prices(instrument_id, date DESC);
CREATE INDEX idx_live_prices_instrument_fetched ON live_prices(instrument_id, fetched_at DESC);
CREATE INDEX idx_news_articles_category_published ON news_articles(category, published_at DESC);
CREATE INDEX idx_news_articles_fetched ON news_articles(fetched_at DESC);
CREATE INDEX idx_sentiment_scores_article ON sentiment_scores(article_id);
CREATE INDEX idx_technical_indicators_instrument_date ON technical_indicators(instrument_id, date DESC);
CREATE INDEX idx_grades_instrument_term ON grades(instrument_id, term, graded_at DESC);

-- Seed instruments
INSERT INTO instruments (symbol, name, category, yfinance_symbol) VALUES
    ('RTX', 'RTX Corporation', 'stock', 'RTX'),
    ('NVDA', 'NVIDIA Corporation', 'stock', 'NVDA'),
    ('GOOGL', 'Alphabet Inc.', 'stock', 'GOOGL'),
    ('AAPL', 'Apple Inc.', 'stock', 'AAPL'),
    ('IITU', 'iShares US Technology ETF', 'etf', 'IITU'),
    ('GOLD', 'Gold Futures', 'commodity', 'GC=F'),
    ('OIL', 'Crude Oil Futures', 'commodity', 'CL=F')
ON CONFLICT (symbol) DO NOTHING;
