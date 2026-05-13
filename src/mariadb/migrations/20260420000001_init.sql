-- Initial schema for crypto-sync.
--
-- We follow a "one table per (symbol, interval)" approach as requested.
-- Tables for specific symbols are created at runtime by the MariaDB adapter
-- (see src/adapters/mariadb_writer.py). This migration only creates the
-- registry table used to track active streams and keep state across restarts.

-- -----------------------------------------------------------------------------
-- stream_registry: one row per (symbol, interval) ever activated.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stream_registry (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    symbol           VARCHAR(32)  NOT NULL,
    `interval`       VARCHAR(8)   NOT NULL,
    status           ENUM('active', 'stopped') NOT NULL DEFAULT 'active',
    table_name       VARCHAR(96)  NOT NULL,
    started_at       BIGINT       NOT NULL COMMENT 'ms since epoch',
    stopped_at       BIGINT       NULL     COMMENT 'ms since epoch',
    last_end_ts      BIGINT       NULL     COMMENT 'last candle close time (ms)',
    extra            BLOB         NULL     COMMENT 'MariaDB Dynamic Columns for free-form metadata',
    PRIMARY KEY (id),
    UNIQUE KEY uk_symbol_interval (symbol, `interval`),
    KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
