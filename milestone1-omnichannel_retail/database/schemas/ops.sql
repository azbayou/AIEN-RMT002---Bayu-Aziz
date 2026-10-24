CREATE SCHEMA IF NOT EXISTS ops;

-- 1. Mencatat lifecycle eksekusi pipeline
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ,
    status TEXT NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    current_stage TEXT,   -- 'validate_raw', 'bronze', 'silver', 'quality_gate', 'gold'
    error_message TEXT,
    records_read INTEGER DEFAULT 0,
    records_clean INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    created_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Mencatat setiap file mentah yang di-ingest, checksum sha256, dan jumlah baris
CREATE TABLE IF NOT EXISTS ops.ingested_files (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    checksum_sha256 TEXT,
    status TEXT NOT NULL, -- 'LOADED', 'FAILED'
    ingested_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Mencatat setiap eksekusi quality check dan audit rule
CREATE TABLE IF NOT EXISTS ops.quality_checks (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    check_stage TEXT NOT NULL, -- 'silver_gate', 'gold_validation'
    passed BOOLEAN NOT NULL,
    details_json JSONB,
    checked_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);