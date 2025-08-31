use std::error::Error;
use std::thread::sleep;
use std::time::{Duration};
use rand::{Rng};
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use common::{get_postgres_pool, mark_tasks_as, read_tasks, uuid, QueueTask, QueueTaskStatus};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    loop {
        let mut set = JoinSet::new();

        tokio::select! {
            _ = signal_interrupt.recv() => {
                set.shutdown().await;
                break
            }
            _ = signal_terminate.recv() => {
                set.shutdown().await;
                break
            }
            tasks = read_tasks(&pool, 4) => {
                for task in tasks? {
                    set.spawn(async move {
                        process_task(&task)
                    });
                }

                let mut completed_task_ids = vec![];
                let mut failed_task_ids = vec![];

                for processed_task_id in set.join_all().await {
                    match processed_task_id {
                        Ok(task_id) => completed_task_ids.push(task_id),
                        Err(task_id) => failed_task_ids.push(task_id),
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

fn process_task(task: &QueueTask) -> Result<uuid::Uuid, uuid::Uuid> {
    let mut rng = rand::rng();

    let processing_seconds = rng.random_range(5..=20);
    sleep(Duration::from_secs(processing_seconds));

    let failure_percentage = rng.random_range(0.0..=1.0);

    if failure_percentage <= 0.3 {
        return Err(task.id)
    }

    Ok(task.id)
}
