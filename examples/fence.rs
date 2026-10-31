use redlock::{ErrorKind, Lock, Options};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctk = CancellationToken::new();
    // Connect to redis.
    let rcli = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let mut rcon = rcli.get_multiplexed_async_connection().await.unwrap();

    // Obtain a lock with a fencing token.
    match Lock::obtain(
        ctk.clone(),
        rcon.clone(),
        String::from("my-key"),
        Duration::from_millis(100),
        Some(Options {
            retry_strategy: redlock::RetryStrategy::NoRetry,
            meta_data: String::new(),
            fence_key: String::from("my-key:fence"),
            token: String::new(),
        }),
    )
    .await
    {
        Ok(lock) => {
            let mut lock = lock;
            println!("I have a lock!");
            // FenceToken is 0 without a FenceKey. Stamp writes with the token; reject older ones.
            let token = lock.fence_token();
            if token > 0 {
                println!("fenced write with token {}\n", token);
            }
            _ = lock.release().await;
            _ = redis::cmd("DEL")
                .arg("my-key:fence")
                .exec_async(&mut rcon)
                .await;
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
