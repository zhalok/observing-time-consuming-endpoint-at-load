-- Runs once, when the Postgres data directory is first initialised.
CREATE TABLE IF NOT EXISTS jobs (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    status     text        NOT NULL CHECK (status IN ('pending', 'processing', 'succeeded', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Serves the per-status counts (server_jobs metric, Grafana "Jobs by status")
-- and the Grafana panels that filter on status plus a time range of updated_at.
-- A (status, updated_at) index also covers status-only lookups.
CREATE INDEX IF NOT EXISTS jobs_status_updated_at_idx ON jobs (status, updated_at);
