use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use common::{get_failed_task_ids, get_postgres_pool, mark_tasks_as, uuid};
use common::QueueTaskStatus;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let number_of_retries = 5;
    let mut reschedule_interval = time::interval(Duration::from_secs(12));

    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    let mut retries_map: HashMap<uuid::Uuid, i32> = HashMap::new();

    loop {
        println!("In memory retries map: {:#?}", retries_map);

        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = reschedule_interval.tick() => {
                println!("Checking for failed tasks...");

                let failed_ids = get_failed_task_ids(&pool).await?;

                retries_map.retain(|id, _| failed_ids.contains(id));

                for failed_id in failed_ids {
                    retries_map.entry(failed_id)
                        .and_modify(|retries| {
                            if *retries > 0 {
                                *retries -= 1;
                            }
                        })
                        .or_insert(number_of_retries);
                }

                let ids_to_reschedule = retries_map.iter()
                    .filter(|(_, v)|  **v > 0)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();

                if ids_to_reschedule.is_empty() {
                    continue;
                }

                println!("The following failed tasks will be rescheduled: {:#?}", ids_to_reschedule);

                mark_tasks_as(&pool, ids_to_reschedule, QueueTaskStatus::Pending).await?;
            }
        }


    }

    pool.close().await;

    Ok(())
}
