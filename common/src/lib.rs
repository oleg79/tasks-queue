use anyhow::{Context, Result};
use std::env;
use fake::{Dummy, Fake, Faker};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::types::Json;
pub use sqlx::types::uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Dummy)]
pub struct QueueTaskPayload {
    pub quality: f32,
    pub length: u32,
    pub title: String,
}

#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "status_enum", rename_all = "lowercase")]
pub enum QueueTaskStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Debug, FromRow)]
pub struct QueueTask {
    pub id: Uuid,
    pub topic: String,
    pub payload: Json<QueueTaskPayload>,
    pub status: QueueTaskStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub async fn get_postgres_pool() -> Result<sqlx::PgPool> {
    let user = env::var("POSTGRES_USER").context("missing POSTGRES_USER")?;
    let pass = env::var("POSTGRES_PASSWORD").context("missing POSTGRES_PASSWORD")?;
    let host = env::var("POSTGRES_HOST").context("missing POSTGRES_HOST")?;
    let port = env::var("POSTGRES_PORT").context("missing POSTGRES_PORT")?;
    let db = env::var("POSTGRES_DB").context("missing POSTGRES_DB")?;

    let postgres_url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(
            env::var("POSTGRES_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
        )
        .connect(&postgres_url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    Ok(pool)
}

pub async fn create_task(pool: &sqlx::PgPool) -> Result<()> {
    #[derive(Debug, Dummy)]
    struct QueueTaskDTO {
        topic: String,
        payload: QueueTaskPayload,
    }

    let queue_task: QueueTaskDTO = Faker.fake();

    let created_task = sqlx::query_as::<_, QueueTask>("
        INSERT INTO tasks (topic, payload) VALUES ($1, $2)
        RETURNING *;
    ")
        .bind(&queue_task.topic)
        .bind(Json(&queue_task.payload))
        .fetch_one(pool)
        .await?;

    tracing::info!(task_id = %created_task.id, "Task created");

    Ok(())
}

pub async fn read_tasks(pool: &sqlx::PgPool, limit: i32) -> Result<Vec<QueueTask>> {
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

pub async fn mark_tasks_as(pool: &sqlx::PgPool, ids: Vec<Uuid>, status: QueueTaskStatus) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }

    tracing::info!(status = ?status, task_ids = ?ids, "Marking tasks");

    let query = sqlx::query("
            UPDATE tasks
            SET status = $1,
                updated_at = now()
            WHERE id = ANY ($2)
    ");

    query
        .bind(status)
        .bind(ids)
        .execute(pool).await?;

    Ok(())
}

pub async fn get_failed_task_ids(pool: &sqlx::PgPool) -> Result<Vec<Uuid>> {
    let query = sqlx::query_scalar("
        SELECT t.id FROM tasks t
        WHERE t.status = 'failed'
    ");

    let ids = query.fetch_all(pool).await?;

    Ok(ids)
}

pub async fn count_tasks_with_status(pool: &sqlx::PgPool, status: QueueTaskStatus) -> Result<i64> {
    let query = sqlx::query_scalar("
        SELECT COUNT(*) FROM tasks t
        WHERE t.status = $1
    ");

    let count = query
        .bind(status)
        .fetch_one(pool).await?;

    Ok(count)
}
