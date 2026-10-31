use redlock::{ErrorKind, Lock, Options};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctk = CancellationToken::new();
    // Connect to redis.
    let rcli = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let rcon = rcli.get_multiplexed_async_connection().await.unwrap();
    let sleep = tokio::time::sleep(Duration::from_mins(1));
    tokio::pin!(sleep);
    // Obtain lock with retry + custom deadline
    loop {
        tokio::select! {
            res = async {
                  Lock::obtain(
                    ctk.clone(),
                    rcon.clone(),
                    String::from("my-key"),
                    Duration::from_millis(100),
                    None,
                ).await
            } => {
                match res {
                    Ok(lock) => {
                        let mut lock = lock;
                        println!("I have a lock!");

                        _ = lock.release().await;
                        return Ok(());
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
             () = &mut sleep => {
                ctk.cancel();
                return Err(ErrorKind::AsyncTaskCancelled.into());
             }
        }
    }
}
