use d_engine::EmbeddedEngine;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let config_path: &str =
        &env::var("CONFIG_PATH").unwrap_or_else(|_| "./config/n1.toml".to_string());

    let engine = EmbeddedEngine::start_with(config_path).await.unwrap();
    let leader_info = match engine.wait_ready(Duration::from_secs(5)).await {
        Ok(info) => info,
        Err(err) => {
            eprintln!("Failed to wait for engine readiness: {err}");
            std::process::exit(1);
        }
    };

    println!("Leader: {}", leader_info.leader_id);

    let client = engine.client();
    let leader_info = *engine.leader_change_notifier().borrow();
    if let Some(info) = leader_info {
        if info.leader_id == engine.node_id() {
            client.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
        } else {
            println!(
                "I'm Follower, node_id={}, leader is {}",
                engine.node_id(),
                info.leader_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let value = client.get_eventual(b"hello".to_vec()).await.unwrap();
    if let Some(v) = value {
        println!("Retrieved: {}", String::from_utf8_lossy(&v));
    } else {
        println!("Key not found");
    }

    // engine.stop().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();
}
