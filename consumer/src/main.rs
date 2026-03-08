use anyhow::Result;
use rand::RngExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use tokio::time::Duration;
use common::{get_postgres_pool, mark_tasks_as, read_tasks, Uuid, QueueTaskStatus};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    let mut set: JoinSet<(Uuid, bool)> = JoinSet::new();

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => {
                while set.join_next().await.is_some() {}
                break;
            }
            _ = signal_terminate.recv() => {
                while set.join_next().await.is_some() {}
                break;
            }
            read_tasks_result = read_tasks(&pool, 7) => {
                // TODO: handle tasks release before spawning if consumer shuts down.
                let tasks = match read_tasks_result {
                    Ok(tasks) if !tasks.is_empty() => tasks,
                    _ => continue,
                };

                for task in tasks {
                    set.spawn(process_task(task.id));
                }

                let mut completed_task_ids = vec![];
                let mut failed_task_ids = vec![];

                while let Some(Ok((id, success))) = set.join_next().await {
                    if success {
                        completed_task_ids.push(id);
                    } else {
                        failed_task_ids.push(id);
                    }
                }

                mark_tasks_as(&pool, completed_task_ids, QueueTaskStatus::Completed).await?;
                mark_tasks_as(&pool, failed_task_ids, QueueTaskStatus::Failed).await?;
            }
        }
    }

    pool.close().await;

    Ok(())
}

async fn process_task(id: Uuid) -> (Uuid, bool) {
    let processing_seconds = rand::rng().random_range(5u64..=20);
    let success = rand::rng().random_range(0.0f64..=1.0) > 0.4;

    tracing::info!(task_id = %id, "Processing task started");

    tokio::time::sleep(Duration::from_secs(processing_seconds)).await;

    tracing::info!(task_id = %id, success, "Processing task finished");

    (id, success)
}
