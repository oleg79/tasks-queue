CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE status_enum AS ENUM ('pending', 'processing', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS tasks(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic TEXT,
    payload JSONB,
    status status_enum DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS tasks_status_id_idx ON tasks (status, id);