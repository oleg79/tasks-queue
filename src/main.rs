use std::error::Error;
use std::env;
use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row};
use sqlx::types::{uuid, Json};

#[derive(Debug, Serialize, Deserialize)]
struct QueueTaskPayload {
    quality: f32,
    codec: String,
    length: u32,
    title: String,
}

impl QueueTaskPayload {
    fn new(quality: f32, codec: &str, length: u32, title: &str) -> Self {
        QueueTaskPayload {
            quality,
            length,
            codec: codec.into(),
            title: title.into(),
        }
    }
}

#[derive(Debug)]
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

async fn read_task(pool: &sqlx::PgPool) -> Result<QueueTask, Box<dyn Error>> {
    let query = sqlx::query_as::<_, QueueTask>("SELECT id, topic, payload, created_at, updated_at FROM tasks");

    let task = query.fetch_one(pool).await?;

    Ok(task)
}

async fn create_task(task: &QueueTaskDTO, pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    sqlx::query("INSERT INTO tasks (topic, payload) VALUES ($1, $2)")
        .bind(&task.topic)
        .bind(Json(&task.payload))
        .execute(pool)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let user = env::var("POSTGRES_USER")?;
    let pass = env::var("POSTGRES_PASSWORD")?;
    let host = env::var("POSTGRES_HOST")?;
    let port = env::var("POSTGRES_PORT")?;
    let db = env::var("POSTGRES_DB")?;

    let postgres_url = format!("postgres://{user}:{pass}@{host}:{port}/{db}",);

    let pool = sqlx::postgres::PgPool::connect(&postgres_url).await?;

    let payload = QueueTaskPayload::new(1.0, "RT9000", 4500, "some-video");

    let queue_task = QueueTaskDTO::new("process-video", payload);

    // create_task(&queue_task, &pool).await?;

    let t = read_task(&pool).await?;

    println!("{:#?}", t);

    Ok(())
}
