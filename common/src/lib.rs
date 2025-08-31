use std::env;
use std::error::Error;
use fake::{Dummy, Fake, Faker};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::types::{uuid, Json};

#[derive(Debug, Serialize, Deserialize, Dummy)]
pub struct QueueTaskPayload {
    pub quality: f32,
    pub length: u32,
    pub title: String,
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
pub struct QueueTask {
    pub id: uuid::Uuid,
    pub topic: String,
    pub payload: Json<QueueTaskPayload>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub async fn get_postgres_pool() -> Result<sqlx::PgPool, Box<dyn Error>> {
    let user = env::var("POSTGRES_USER")?;
    let pass = env::var("POSTGRES_PASSWORD")?;
    let host = env::var("POSTGRES_HOST")?;
    let port = env::var("POSTGRES_PORT")?;
    let db = env::var("POSTGRES_DB")?;

    let postgres_url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");

    let pool = sqlx::postgres::PgPool::connect(&postgres_url).await?;

    Ok(pool)
}

pub async fn create_task(pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    let queue_task: QueueTaskDTO = Faker.fake();

    let created_task = sqlx::query_as::<_, QueueTask>("
        INSERT INTO tasks (topic, payload) VALUES ($1, $2)
        RETURNING *;
    ")
        .bind(&queue_task.topic)
        .bind(Json(&queue_task.payload))
        .fetch_one(pool)
        .await?;

    println!("Task({}) created.", created_task.id);

    Ok(())
}

pub async fn read_tasks(pool: &sqlx::PgPool, limit: i32) -> Result<Vec<QueueTask>, Box<dyn Error>> {
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