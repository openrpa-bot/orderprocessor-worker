-- Database schema for openalgo_optionchain table (PostgreSQL/Citus compatible)
-- Run this SQL script to create the table
-- CE and PE data are stored in a single row per strike

CREATE TABLE IF NOT EXISTS openalgo_optionchain (
    id BIGSERIAL PRIMARY KEY,
    server_name VARCHAR(100) NOT NULL,
    underlying VARCHAR(50) NOT NULL,
    underlying_ltp NUMERIC(15, 2),
    underlying_prev_close NUMERIC(15, 2),
    expiry_date VARCHAR(20) NOT NULL,
    atm_strike INTEGER,
    strike INTEGER NOT NULL,
    -- CE Option fields
    ce_symbol VARCHAR(100),
    ce_label VARCHAR(20),
    ce_ltp NUMERIC(15, 2),
    ce_bid NUMERIC(15, 2),
    ce_ask NUMERIC(15, 2),
    ce_open NUMERIC(15, 2),
    ce_high NUMERIC(15, 2),
    ce_low NUMERIC(15, 2),
    ce_prev_close NUMERIC(15, 2),
    ce_volume BIGINT,
    ce_oi BIGINT,
    ce_spot_price NUMERIC(15, 2),
    ce_option_price NUMERIC(15, 2),
    ce_implied_volatility NUMERIC(10, 4),
    ce_days_to_expiry NUMERIC(10, 2),
    ce_delta NUMERIC(10, 6),
    ce_gamma NUMERIC(10, 6),
    ce_theta NUMERIC(10, 6),
    ce_vega NUMERIC(10, 6),
    -- PE Option fields
    pe_symbol VARCHAR(100),
    pe_label VARCHAR(20),
    pe_ltp NUMERIC(15, 2),
    pe_bid NUMERIC(15, 2),
    pe_ask NUMERIC(15, 2),
    pe_open NUMERIC(15, 2),
    pe_high NUMERIC(15, 2),
    pe_low NUMERIC(15, 2),
    pe_prev_close NUMERIC(15, 2),
    pe_volume BIGINT,
    pe_oi BIGINT,
    pe_spot_price NUMERIC(15, 2),
    pe_option_price NUMERIC(15, 2),
    pe_implied_volatility NUMERIC(10, 4),
    pe_days_to_expiry NUMERIC(10, 2),
    pe_delta NUMERIC(10, 6),
    pe_gamma NUMERIC(10, 6),
    pe_theta NUMERIC(10, 6),
    pe_vega NUMERIC(10, 6),
    -- Common fields
    lotsize INTEGER,
    tick_size NUMERIC(10, 2),
    datetime TIMESTAMP NOT NULL
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_server_underlying_expiry ON openalgo_optionchain (server_name, underlying, expiry_date);
CREATE INDEX IF NOT EXISTS idx_datetime ON openalgo_optionchain (datetime);
CREATE INDEX IF NOT EXISTS idx_strike ON openalgo_optionchain (strike);
CREATE INDEX IF NOT EXISTS idx_ce_symbol ON openalgo_optionchain (ce_symbol);
CREATE INDEX IF NOT EXISTS idx_pe_symbol ON openalgo_optionchain (pe_symbol);
