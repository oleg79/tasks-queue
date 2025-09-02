use std::{env, fs};
use std::error::Error;
use std::ops::{Div};
use std::time::Duration;
use bollard::{Docker, API_DEFAULT_VERSION};
use bollard::models::{ContainerCreateBody, HostConfig, NetworkingConfig};
use bollard::query_parameters::{CreateContainerOptionsBuilder, InspectContainerOptions, ListContainersOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptionsBuilder, StopContainerOptionsBuilder};
use nanoid::nanoid;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use common::{count_tasks_with_status, get_postgres_pool, QueueTaskStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let hostname = fs::read_to_string("/etc/hostname")?;

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
    )?;

    let parent_options = docker.inspect_container(hostname.as_str(), None::<InspectContainerOptions>).await?;

    let parent_networks = parent_options.network_settings.ok_or("")?.networks;

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

                let needed_consumer_workers_count = pending_tasks_count.div(number_of_tasks_per_consumer) as usize;

                 let all_running_containers = docker
                    .list_containers(ListContainersOptionsBuilder::default().all(false).build().into())
                    .await?;

                let actual_consumer_workers_count = all_running_containers
                    .iter()
                    .filter(|c| c.image.as_deref() == Some(consumer_image_name))
                    .count();

                if needed_consumer_workers_count > actual_consumer_workers_count {
                    let consumer_workers_to_spawn_count = 0.max(needed_consumer_workers_count - actual_consumer_workers_count);

                    println!("{} consumer workers required.", consumer_workers_to_spawn_count);

                    for _ in 0..consumer_workers_to_spawn_count {
                        let container_id = nanoid!(10);
                        let container_name = format!("consumer-worker-{}", container_id);

                        println!("Starting consumer worker({})...", container_name);

                        let options = CreateContainerOptionsBuilder::default()
                            .name(container_name.as_str())
                            .build();

                        docker.create_container(
                            options.into(),
                            config.clone(),
                        ).await?;

                        docker
                            .start_container(container_name.as_str(), Some(StartContainerOptionsBuilder::default().build()))
                            .await?;

                        println!("Started consumer worker({}).", container_name);

                        running_worker_names.push(container_name);
                    }
                } else if needed_consumer_workers_count < actual_consumer_workers_count {
                    let consumer_workers_to_shut_down = 0.max(actual_consumer_workers_count - needed_consumer_workers_count);

                    for _ in 0..consumer_workers_to_shut_down {
                        if let Some(container_name) = running_worker_names.pop() {
                            println!("Shutting down consumer worker({})...", container_name);

                            docker.stop_container(
                                container_name.as_str(),
                                Some(StopContainerOptionsBuilder::default().t(4).build()),
                            ).await?;

                            let response = docker.inspect_container(container_name.as_str(), None::<InspectContainerOptions>).await?;

                            while let Some(s) = &response.state {
                                if s.running == Some(false) {
                                    break;
                                } else {
                                    time::sleep(Duration::from_millis(200)).await;
                                }
                            }

                            docker.remove_container(
                                container_name.as_str(),
                                Some(
                                    RemoveContainerOptionsBuilder::default()
                                        .v(true)
                                        .force(true)
                                        .build()
                                ),
                            ).await?;

                            println!("Shut down consumer worker({}).", container_name);
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
