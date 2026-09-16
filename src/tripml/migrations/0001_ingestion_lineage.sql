CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id UUID PRIMARY KEY,
    dataset TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    source_url TEXT,
    source_path TEXT NOT NULL,
    source_sha256 CHAR(64),
    config_fingerprint CHAR(64) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'accepted', 'quarantined', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    total_rows BIGINT,
    valid_rows BIGINT,
    invalid_rows BIGINT,
    violation_rate DOUBLE PRECISION,
    contract_version TEXT,
    silver_path TEXT,
    quarantine_path TEXT,
    error_type TEXT,
    error_message TEXT,
    CHECK (total_rows IS NULL OR total_rows = valid_rows + invalid_rows),
    CHECK (violation_rate IS NULL OR violation_rate BETWEEN 0 AND 1),
    CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS ingestion_runs_partition_started_idx
    ON ingestion_runs (dataset, partition_key, started_at DESC);

CREATE INDEX IF NOT EXISTS ingestion_runs_status_idx
    ON ingestion_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_quality_checks (
    run_id UUID NOT NULL REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
    rule TEXT NOT NULL,
    violations BIGINT NOT NULL CHECK (violations >= 0),
    total_rows BIGINT NOT NULL CHECK (total_rows >= 0),
    threshold DOUBLE PRECISION NOT NULL CHECK (threshold BETWEEN 0 AND 1),
    passed BOOLEAN NOT NULL,
    contract_version TEXT NOT NULL,
    PRIMARY KEY (run_id, rule),
    CHECK (violations <= total_rows)
);
