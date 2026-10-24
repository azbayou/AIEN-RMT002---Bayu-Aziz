CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    source_category TEXT NOT NULL, -- 'operational', 'events', 'inventory', 'reference'
    file_name TEXT NOT NULL,
    source_row_id TEXT,
    payload JSONB NOT NULL,
    loaded_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bronze_run_file 
ON bronze.raw_records (pipeline_run_id, file_name);

CREATE INDEX IF NOT EXISTS idx_bronze_source_row 
ON bronze.raw_records (source_row_id);