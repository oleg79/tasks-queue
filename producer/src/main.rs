use std::error::Error;
use std::env;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row};
use sqlx::types::{uuid, Json};
use fake::{Dummy, Fake, Faker};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{interval, Duration, MissedTickBehavior};

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

async fn read_tasks(pool: &sqlx::PgPool) -> Result<Vec<QueueTask>, Box<dyn Error>> {
    let query = sqlx::query_as::<_, QueueTask>("SELECT id, topic, payload, created_at, updated_at FROM tasks");

    let tasks = query.fetch_all(pool).await?;

    Ok(tasks)
}

async fn create_task(task: &QueueTaskDTO, pool: &sqlx::PgPool) -> Result<QueueTask, Box<dyn Error>> {
    let task = sqlx::query_as::<_, QueueTask>("
        INSERT INTO tasks (topic, payload) VALUES ($1, $2)
        RETURNING *;
    ")
        .bind(&task.topic)
        .bind(Json(&task.payload))
        .fetch_one(pool)
        .await?;

    Ok(task)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let mut ticker = interval(Duration::from_secs(2));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let postgres_url = get_postgres_url()?;
    let pool = sqlx::postgres::PgPool::connect(&postgres_url).await?;

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = ticker.tick() => {
                let queue_task: QueueTaskDTO = Faker.fake();

                let created_task = create_task(&queue_task, &pool).await?;
                println!("Task({}) created.", created_task.id);
            }
        }
    }

    pool.close().await;

    Ok(())
}

fn get_postgres_url() -> Result<String, Box<dyn Error>> {
    let user = env::var("POSTGRES_USER")?;
    let pass = env::var("POSTGRES_PASSWORD")?;
    let host = env::var("POSTGRES_HOST")?;
    let port = env::var("POSTGRES_PORT")?;
    let db = env::var("POSTGRES_DB")?;

    let postgres_url = format!("postgres://{user}:{pass}@{host}:{port}/{db}");

    Ok(postgres_url)
}
