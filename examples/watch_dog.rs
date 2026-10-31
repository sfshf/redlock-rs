use opentelemetry::trace::FutureExt;
use redlock::{ErrorKind, Lock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctk = CancellationToken::new();
    // Connect to redis.
    let rcli = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let rcon = rcli.get_multiplexed_async_connection().await.unwrap();

    // Obtain a lock with a 30s TTL.
    let ttl = Duration::from_secs(3);
    match Lock::obtain(ctk.clone(), rcon, String::from("my-key"), ttl, None).await {
        Ok(lock) => {
            let mut lock = lock;
            println!("I have a lock!");
            // Start a watchdog that refreshes the lock every ttl/3. The work context
            // is cancelled if a refresh fails so the protected work can abort.
            let ticker = tokio::time::interval(ttl / 3);
            tokio::pin!(ticker);
            // Mock another async routine to cancel the process.
            let sleep = tokio::time::sleep(Duration::from_secs(15));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Extend my lock.
                        match lock.refresh(ctk.clone(), ttl, None).await {
                            Ok(_)=>{
                                println!("lock refresh success");
                            }
                            Err(err)=>{
                                println!("lock refresh failed: {}",err);
                                ctk.cancel();
                                break;
                            }
                        }
                    }
                    _ = ctk.cancelled() => {
                        println!("watch dog is cancelled");
                        _ = lock.release().await;
                        break;
                    }
                   () = &mut sleep => {
                        println!("time out, executing cancel operation");
                        ctk.cancel();
                   }
                }
            }

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
