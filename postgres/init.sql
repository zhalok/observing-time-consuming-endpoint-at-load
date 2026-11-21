-- Runs once, when the Postgres data directory is first initialised.
CREATE TABLE IF NOT EXISTS jobs (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    status     text        NOT NULL CHECK (status IN ('pending', 'processing', 'succeeded', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
