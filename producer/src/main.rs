use std::error::Error;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{interval, Duration, MissedTickBehavior};
use common::{get_postgres_pool, create_task};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut signal_interrupt = signal(SignalKind::interrupt())?;
    let mut signal_terminate = signal(SignalKind::terminate())?;

    let mut ticker = interval(Duration::from_secs(2));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let pool = get_postgres_pool().await?;

    loop {
        tokio::select! {
            _ = signal_interrupt.recv() => break,
            _ = signal_terminate.recv() => break,
            _ = ticker.tick() => create_task(&pool).await?,
        }
    }

    pool.close().await;

    Ok(())
}
