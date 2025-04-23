use std::time::Duration;

use d_engine::client::Client;
use d_engine::client::ClientBuilder;
use d_engine::convert::kv;
use d_engine::convert::vk;
use d_engine::proto::ErrorCode;
use d_engine::proto::NodeMeta;
use d_engine::ClientApiError;
use d_engine::Result;
use d_engine::LEADER;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::common::ClientCommands;
use crate::common::{self};

const MAX_RETRIES: u32 = 10;
const RETRY_DELAY_MS: u64 = 50;

#[derive(Clone)]
pub struct ClientManager {
    client: Client,
}

impl ClientManager {
    pub async fn new(bootstrap_urls: &[String]) -> std::result::Result<Self, ClientApiError> {
        let bootstrap_urls = bootstrap_urls.to_vec();

        let client = match ClientBuilder::new(bootstrap_urls.clone())
            .connect_timeout(Duration::from_secs(600))
            .request_timeout(Duration::from_secs(600))
            .enable_compression(true)
            .build()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build client: {:?}", e);
                return Err(e);
            }
        };

        Ok(Self { client })
    }

    /// Update Leader client (polling all nodes)
    async fn refresh_client(&mut self) -> std::result::Result<(), ClientApiError> {
        self.client.refresh(None).await
    }

    pub async fn execute_command(
        &mut self,
        command: common::ClientCommands,
        key: u64,
        value: Option<u64>,
    ) -> std::result::Result<u64, ClientApiError> {
        debug!("recevied command = {:?}", &command);
        let mut retries = 0;
        loop {
            // Handle subcommands
            match command {
                ClientCommands::Put => {
                    let value = value.unwrap();

                    info!("put {}:{}", key, value);

                    match self.client.kv().put(kv(key), kv(value)).await {
                        Ok(res) => {
                            debug!("Put Success: {:?}", res);
                            return Ok(key);
                        }
                        Err(e) if e.code().eq(&(ErrorCode::NotLeader as u32)) && retries < MAX_RETRIES => {
                            retries += 1;
                            self.refresh_client().await?;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(e) if e.code().eq(&(ErrorCode::ConnectionTimeout as u32)) && retries < MAX_RETRIES => {
                            retries += 1;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(e) => {
                            error!("ClientCommands::Put, ErrorCode = {:?}", e.code());
                            return Err(e);
                        }
                    }
                }
                ClientCommands::Delete => match self.client.kv().delete(kv(key)).await {
                    Ok(res) => {
                        debug!("Delete Success: {:?}", res);
                        return Ok(key);
                    }
                    Err(e) if e.code().eq(&(ErrorCode::NotLeader as u32)) && retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) if e.code().eq(&(ErrorCode::ConnectionTimeout as u32)) && retries < MAX_RETRIES => {
                        retries += 1;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        return Err(e);
                    }
                },
                ClientCommands::Read => match self.client.kv().get(kv(key), false).await? {
                    Some(r) => {
                        let v = vk(&r.value);
                        debug!("Success: {:?}", v);
                        return Ok(v);
                    }
                    None => {
                        error!("No entry found for key: {}", key);
                        return Err(ErrorCode::KeyNotExist.into());
                    }
                },
                ClientCommands::Lread => match self.client.kv().get(kv(key), true).await {
                    Ok(result) => match result {
                        Some(r) => {
                            let v = vk(&r.value);
                            debug!("Success: {:?}", v);
                            return Ok(v);
                        }
                        None => {
                            error!("No entry found for key: {}", key);
                            return Err(ErrorCode::KeyNotExist.into());
                        }
                    },
                    Err(e) if e.code().eq(&(ErrorCode::NotLeader as u32)) && retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) if e.code().eq(&(ErrorCode::ConnectionTimeout as u32)) && retries < MAX_RETRIES => {
                        retries += 1;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) => {
                        error!("Error Code = {:?}", e.code());
                        return Err(e);
                    }
                },
            }
        }
    }

    // Helper function to verify linearizable reads
    pub async fn verify_read(
        &mut self,
        key: u64,
        expected_value: u64,
        iterations: u64,
    ) {
        println!("read: {}", key);
        for _ in 0..iterations {
            assert_eq!(
                self.execute_command(ClientCommands::Lread, key, None).await.unwrap(),
                expected_value,
                "Linearizable read failed for key {}!",
                key
            );
        }
    }

    pub async fn list_members(&self) -> Result<Vec<NodeMeta>> {
        self.client.cluster().list_members().await
    }
    pub async fn list_leader_id(&self) -> Result<u32> {
        let members = self.list_members().await?;
        let mut ids: Vec<u32> = members
            .iter()
            .filter(|meta| meta.role == LEADER)
            .map(|n| n.id)
            .collect();

        Ok(ids.pop().unwrap_or(0))
    }
}
