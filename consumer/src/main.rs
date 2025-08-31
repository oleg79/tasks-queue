use std::error::Error;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{interval, MissedTickBehavior};
use common::{get_postgres_pool, read_tasks};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let mut ticker = interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let pool = get_postgres_pool().await?;

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = ticker.tick() => {
                let tasks_to_rocess = read_tasks(&pool, 4).await?;

                println!("Fetched tasks: {}", tasks_to_rocess.len());
            }
        }
    }

    pool.close().await;

    Ok(())
}
