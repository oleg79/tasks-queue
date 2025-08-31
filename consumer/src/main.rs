use std::env;
use std::error::Error;
use std::time::Duration;
use fake::Dummy;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::types::{uuid, Json};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{interval, MissedTickBehavior};

#[derive(Debug, Serialize, Deserialize, Dummy)]
struct QueueTaskPayload {
    quality: f32,
    length: u32,
    title: String,
}

impl QueueTaskPayload {
    fn new(quality: f32, length: u32, title: &str) -> Self {
        QueueTaskPayload {
            quality,
            length,
            title: title.into(),
        }
    }
}

#[derive(Debug, Dummy)]
struct QueueTaskDTO {
    topic: String,
    payload: QueueTaskPayload,
}

impl QueueTaskDTO {
    fn new(topic: &str, payload: QueueTaskPayload) -> Self {
        QueueTaskDTO {
            topic: topic.into(),
            payload
        }
    }
}

#[derive(Debug, FromRow)]
struct QueueTask {
    id: uuid::Uuid,
    topic: String,
    payload: Json<QueueTaskPayload>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

async fn read_tasks(pool: &sqlx::PgPool, limit: i32) -> Result<Vec<QueueTask>, Box<dyn Error>> {
    let query = sqlx::query_as::<_, QueueTask>("
        WITH pooled AS (
          UPDATE tasks t
          SET status = 'processing',
              updated_at = now()
          WHERE id IN (
            SELECT id
            FROM tasks
            WHERE status = 'pending' AND pg_try_advisory_lock(hashtext(id::text))
            ORDER BY id
            LIMIT $1
          )
          RETURNING t.id, t.topic, t.payload, t.status, t.created_at, t.updated_at
        )
        SELECT * FROM pooled;
    ");

    let tasks = query.bind(limit).fetch_all(pool).await?;

    Ok(tasks)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let mut ticker = interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let pool = get_postgres_pool().await?;

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = ticker.tick() => {
                let tasks_to_rocess = read_tasks(&pool, 4).await?;

                println!("Fetched tasks: {}", tasks_to_rocess.len());
            }
        }
    }

    pool.close().await;

    Ok(())
}

async fn get_postgres_pool() -> Result<sqlx::PgPool, Box<dyn Error>> {
    let user = env::var("POSTGRES_USER")?;
    let pass = env::var("POSTGRES_PASSWORD")?;
    let host = env::var("POSTGRES_HOST")?;
    let port = env::var("POSTGRES_PORT")?;
    let db = env::var("POSTGRES_DB")?;

    let postgres_url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");

    let pool = sqlx::postgres::PgPool::connect(&postgres_url).await?;

    Ok(pool)
}
