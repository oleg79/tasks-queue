use std::error::Error;
use std::ops::Div;
use std::time::Duration;
use bollard::{Docker, API_DEFAULT_VERSION};
use bollard::query_parameters::ListContainersOptionsBuilder;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use common::{count_tasks_with_status, get_postgres_pool, QueueTaskStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let consumer_image_name = "tasks-queue-consumer";
    let number_of_tasks_per_consumer: i64 = 500;
    let mut load_check_interval = time::interval(Duration::from_secs(20));

    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    let docker = Docker::connect_with_unix(
        "/var/run/docker.sock",
        120,
        API_DEFAULT_VERSION,
    )?;

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = load_check_interval.tick() => {
                let pending_tasks_count = count_tasks_with_status(&pool, QueueTaskStatus::Pending).await?;

                let needed_consumer_workers_count = pending_tasks_count.div(number_of_tasks_per_consumer) as usize;

                 let all_running_containers = docker
                    .list_containers(ListContainersOptionsBuilder::default().all(false).build().into())
                    .await?;

                let actual_consumer_workers_count = all_running_containers
                    .iter()
                    .filter(|c| c.image.as_deref() == Some(consumer_image_name))
                    .count();

                let consumer_workers_to_spawn_count = 0.max(needed_consumer_workers_count - actual_consumer_workers_count);

                println!("{} consumer workers required.", consumer_workers_to_spawn_count);
            }
        }
    }

    pool.close().await;

    Ok(())
}
