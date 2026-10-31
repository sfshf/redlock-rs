use redlock::{ErrorKind, Lock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctk = CancellationToken::new();
    // Connect to redis.
    let rcli = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let rcon = rcli.get_multiplexed_async_connection().await.unwrap();

    // Try to obtain lock.
    match Lock::obtain(
        ctk.clone(),
        rcon,
        String::from("my-key"),
        Duration::from_millis(100),
        None,
    )
    .await
    {
        Ok(lock) => {
            let mut lock = lock;
            println!("I have a lock!");
            // Sleep and check the remaining TTL.
            tokio::time::sleep(Duration::from_millis(50)).await;
            match lock.ttl().await {
                Ok(ttl) => {
                    if ttl > Duration::ZERO {
                        println!("Yay, I still have my lock!");
                    } else {
                        println!("[ERROR] my lock has expired!");
                    }
                }
                Err(err) => {
                    // Don't forget to release
                    _ = lock.release().await;
                    panic!("{}", err);
                }
            }

            // Extend my lock.
            lock.refresh(ctk, Duration::from_millis(100), None)
                .await
                .unwrap();

            // Sleep a little longer, then check
            tokio::time::sleep(Duration::from_millis(100)).await;
            match lock.ttl().await {
                Ok(ttl) => {
                    if ttl == Duration::ZERO {
                        println!("Now, my lock has expired!");
                    } else {
                        println!("[ERROR] i still have my lock!");
                    }
                }
                Err(err) => {
                    // Don't forget to release
                    _ = lock.release().await;
                    panic!("{}", err);
                }
            }

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
