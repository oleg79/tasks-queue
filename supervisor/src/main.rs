use anyhow::{Context, Result};
use std::{env, fs};
use std::time::Duration;
use bollard::{Docker, API_DEFAULT_VERSION};
use bollard::models::{ContainerCreateBody, HostConfig, NetworkingConfig};
use bollard::query_parameters::{CreateContainerOptionsBuilder, InspectContainerOptions, ListContainersOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptionsBuilder, StopContainerOptionsBuilder};
use nanoid::nanoid;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use common::{count_tasks_with_status, get_postgres_pool, QueueTaskStatus};

const MAX_STOP_WAIT: Duration = Duration::from_secs(30);

async fn start_consumer(docker: &Docker, config: &ContainerCreateBody) -> Result<String> {
    let container_name = format!("consumer-worker-{}", nanoid!(10));

    tracing::info!(container = %container_name, "Starting consumer worker");

    let options = CreateContainerOptionsBuilder::default()
        .name(container_name.as_str())
        .build();

    docker.create_container(options.into(), config.clone())
        .await
        .with_context(|| format!("Failed to create container {}", container_name))?;

    docker
        .start_container(container_name.as_str(), Some(StartContainerOptionsBuilder::default().build()))
        .await
        .with_context(|| format!("Failed to start container {}", container_name))?;

    tracing::info!(container = %container_name, "Started consumer worker");

    Ok(container_name)
}

async fn stop_consumer(docker: &Docker, container_name: &str) -> Result<()> {
    tracing::info!(container = %container_name, "Shutting down consumer worker");

    docker.stop_container(
        container_name,
        Some(StopContainerOptionsBuilder::default().t(4).build()),
    )
    .await
    .with_context(|| format!("Failed to stop container {}", container_name))?;

    let deadline = time::Instant::now() + MAX_STOP_WAIT;

    loop {
        let running = docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
            .with_context(|| format!("Failed to inspect container {}", container_name))?
            .state
            .and_then(|s| s.running);

        if running == Some(false) {
            break;
        }

        if time::Instant::now() >= deadline {
            tracing::warn!(container = %container_name, "Container did not stop in 30s, forcing removal");
            break;
        }

        time::sleep(Duration::from_millis(200)).await;
    }

    docker.remove_container(
        container_name,
        Some(
            RemoveContainerOptionsBuilder::default()
                .v(true)
                .force(true)
                .build()
        ),
    )
    .await
    .with_context(|| format!("Failed to remove container {}", container_name))?;

    tracing::info!(container = %container_name, "Shut down consumer worker");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let hostname = fs::read_to_string("/etc/hostname")?.trim().to_string();

    let consumer_image_name = "tasks-queue-consumer";
    let number_of_tasks_per_consumer: i64 = 200;
    let mut load_check_interval = time::interval(Duration::from_secs(20));

    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let pool = get_postgres_pool().await?;

    let docker = Docker::connect_with_unix(
        "/var/run/docker.sock",
        120,
        API_DEFAULT_VERSION,
    )
    .context("Failed to connect to Docker daemon")?;

    let parent_options = docker
        .inspect_container(hostname.as_str(), None::<InspectContainerOptions>)
        .await
        .with_context(|| format!("Failed to inspect container {}", hostname))?;

    let parent_networks = parent_options.network_settings.context("missing network_settings")?.networks;

    let mut env = vec![];

    for var in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"] {
        env.push(format!("{}={}", var, env::var(var)?))
    }

    let networking_config = NetworkingConfig { endpoints_config: parent_networks };

    let host_config = HostConfig {
        network_mode: Some("tasks-queue_default".to_string()),
        ..Default::default()
    };

    let config = ContainerCreateBody {
        image: Some(consumer_image_name.into()),
        env: Some(env),
        networking_config: Some(networking_config),
        host_config: Some(host_config),
        ..Default::default()
    };

    let mut running_worker_names = vec![];

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = load_check_interval.tick() => {
                let pending_tasks_count = count_tasks_with_status(&pool, QueueTaskStatus::Pending).await?;

                let needed_consumer_workers_count = if pending_tasks_count == 0 {
                    0
                } else {
                    ((pending_tasks_count + number_of_tasks_per_consumer - 1) / number_of_tasks_per_consumer) as usize
                };

                let all_running_containers = docker
                    .list_containers(ListContainersOptionsBuilder::default().all(false).build().into())
                    .await?;

                let actual_consumer_workers_count = all_running_containers
                    .iter()
                    .filter(|c| c.image.as_deref() == Some(consumer_image_name))
                    .count();

                if needed_consumer_workers_count > actual_consumer_workers_count {
                    let to_spawn = needed_consumer_workers_count - actual_consumer_workers_count;
                    tracing::info!(count = to_spawn, "Consumer workers required");

                    for _ in 0..to_spawn {
                        let container_name = start_consumer(&docker, &config).await?;
                        running_worker_names.push(container_name);
                    }
                } else if needed_consumer_workers_count < actual_consumer_workers_count {
                    let to_stop = actual_consumer_workers_count - needed_consumer_workers_count;

                    for _ in 0..to_stop {
                        if let Some(container_name) = running_worker_names.pop() {
                            stop_consumer(&docker, &container_name).await?;
                        }
                    }
                } else {
                    continue;
                }
            }
        }
    }

    pool.close().await;

    Ok(())
}
