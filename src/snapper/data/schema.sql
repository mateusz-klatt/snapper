-- Illustrative schema (actual schema managed by Alembic)
CREATE TABLE IF NOT EXISTS instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE,
    base TEXT,
    quote TEXT,
    tick_size REAL,
    lot_size REAL
);

CREATE TABLE IF NOT EXISTS process_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id VARCHAR(36) NOT NULL UNIQUE,
    process_name VARCHAR(64) NOT NULL,
    role VARCHAR(16) NOT NULL,
    lifecycle VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL,
    parameters JSON,
    result JSON,
    error VARCHAR(1024),
    tags JSON,
    started_at DATETIME NOT NULL,
    completed_at DATETIME
);

CREATE INDEX IF NOT EXISTS ix_process_runs_process_name ON process_runs (process_name);
CREATE INDEX IF NOT EXISTS ix_process_runs_status ON process_runs (status);
CREATE INDEX IF NOT EXISTS ix_process_runs_started_at ON process_runs (started_at);
