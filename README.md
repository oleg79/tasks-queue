# tasks-queue

A self-scaling task queue built in Rust, backed by PostgreSQL, orchestrated with Docker.

## Architecture

| Service | Role |
|---|---|
| **producer** | Inserts one random task into the DB every 2 seconds |
| **consumer** | Claims up to 7 tasks at a time (advisory locks), processes each for 5–20 s, marks completed or failed |
| **rescheduler** | Every 12 s, resets failed tasks to `pending`; gives up after 5 retries |
| **supervisor** | Every 20 s, counts pending tasks and spawns/removes consumer containers via the Docker API (`pending / 200` target workers) |
| **db** | PostgreSQL — stores tasks with JSONB payload and a `status_enum` column |

```
producer ──► db (tasks table)
                │
consumer ◄──────┤  (pg_try_advisory_lock, batch of 7)
                │
rescheduler ◄───┘  (reset failed → pending, up to 5 retries)

supervisor ──► Docker API ──► spawn/remove consumer containers
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose

## Running

```sh
docker compose up --build
```

The supervisor will automatically scale consumer workers based on queue depth.

## How it works

1. **producer** generates fake tasks (`topic`, `payload: {quality, length, title}`) and inserts them every 2 s.
2. **consumer** uses `pg_try_advisory_lock` for optimistic concurrent claim — no task is double-processed across worker containers. Each batch of 7 tasks is processed in parallel Tokio tasks. ~40% of tasks fail randomly to exercise the retry path.
3. **rescheduler** keeps an in-memory retry counter per task UUID. Failed tasks are re-queued up to 5 times; after that they stay `failed`.
4. **supervisor** mounts `/var/run/docker.sock` and uses the Bollard Docker client to create and remove `tasks-queue-consumer` containers. Target count = `floor(pending_count / 200)`.

## Configuration

Copy `.env` and adjust as needed:

```env
POSTGRES_USER=queue-user
POSTGRES_PASSWORD=queue-pass
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=tasks-queue
```

## Schema

```sql
CREATE TABLE tasks (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic      TEXT,
    payload    JSONB,
    status     status_enum DEFAULT 'pending',  -- pending | processing | completed | failed
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
