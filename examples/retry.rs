use redlock::{ErrorKind, Lock, Options};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctk = CancellationToken::new();
    // Connect to redis.
    let rcli = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let rcon = rcli.get_multiplexed_async_connection().await.unwrap();

    // Retry every 100ms, for up-to 3x
    match Lock::obtain(
        ctk.clone(),
        rcon,
        String::from("my-key"),
        Duration::from_secs(1),
        Some(Options {
            retry_strategy: redlock::RetryStrategy::LimitedRetry(Box::new(
                redlock::LimitedRetry::new(
                    redlock::RetryStrategy::LinearBackoff(Duration::from_millis(100)),
                    3,
                ),
            )),
            meta_data: String::new(),
            fence_key: String::new(),
            token: String::new(),
        }),
    )
    .await
    {
        Ok(lock) => {
            let mut lock = lock;
            println!("I have a lock!");
            _ = lock.release().await;
            Ok(())
        }
        Err(err) => {
            if let Some(e) = err.downcast_ref::<ErrorKind>() {
                if e.eq(&ErrorKind::NotObtained) {
                    println!("Could not obtain lock!");
                }
                return Err(err);
            } else {
                panic!("not custom error");
            }
        }
    }
}
