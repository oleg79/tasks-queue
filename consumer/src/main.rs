use std::error::Error;
use std::thread::sleep;
use std::time::{Duration};
use rand::{Rng};
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use common::{get_postgres_pool, mark_tasks_as, read_tasks, uuid, QueueTask, QueueTaskStatus};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut no_tasks_to_process_cycles = 5;

    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    loop {
        if no_tasks_to_process_cycles == 0 {
            break;
        }

        let mut set = JoinSet::new();

        tokio::select! {
            _ = signal_interrupt.recv() => {
                set.shutdown().await;
                break;
            }
            _ = signal_terminate.recv() => {
                set.shutdown().await;
                break;
            }
            read_tasks_result = read_tasks(&pool, 7) => {
                let tasks = match read_tasks_result {
                    Ok(tasks) if !tasks.is_empty() => tasks,
                    _ => {
                        no_tasks_to_process_cycles -= 1;
                        let seconds_till_shutdown = 20 - (5 - no_tasks_to_process_cycles) * 4;

                        if seconds_till_shutdown == 0 {
                            println!("Shutting down...");
                        } else {
                            println!(
                                "No tasks to process. Waiting for {}s before shutting down.",
                                seconds_till_shutdown
                            );
                        }


                        tokio::time::sleep(Duration::from_secs(4)).await;
                        continue;
                    }
                };
                no_tasks_to_process_cycles = 5;

                for task in tasks {
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
