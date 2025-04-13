use crate::common::{self, ClientCommands};
use d_engine::{
    client::{Client, ClientBuilder},
    convert::{kv, vk},
    proto::NodeMeta,
    Error, Result, LEADER,
};
use log::{debug, error, info};
use std::time::Duration;
use tokio::time::sleep;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 50;

#[derive(Clone)]
pub struct ClientManager {
    client: Client,
}

impl ClientManager {
    pub async fn new(bootstrap_urls: &[String]) -> Result<Self> {
        let bootstrap_urls = bootstrap_urls.to_vec();

        let client = match ClientBuilder::new(bootstrap_urls.clone())
            .connect_timeout(Duration::from_secs(120))
            .request_timeout(Duration::from_secs(120))
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
    async fn refresh_client(&mut self) -> Result<()> {
        self.client.refresh(None).await
    }

    pub async fn execute_command(
        &mut self,
        command: common::ClientCommands,
        key: u64,
        value: Option<u64>,
    ) -> Result<u64> {
        debug!("recevied command = {:?}", &command);
        let mut retries = 0;
        loop {
            // Handle subcommands
            match command {
                ClientCommands::PUT => {
                    let value = value.unwrap();

                    info!("put {}:{}", key, value);

                    match self.client.kv().put(kv(key), kv(value)).await {
                        Ok(res) => {
                            debug!("Put Success: {:?}", res);
                            return Ok(key);
                        }
                        Err(Error::NodeIsNotLeaderError) if retries < MAX_RETRIES => {
                            retries += 1;
                            self.refresh_client().await?;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(Error::ClientRequestCanceledError) if retries < MAX_RETRIES => {
                            retries += 1;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(e) => {
                            error!("Error: {:?}", e);
                            return Err(Error::GeneralClientError(format!("Error: {:?}", e)));
                        }
                    }
                }
                ClientCommands::DELETE => match self.client.kv().delete(kv(key)).await {
                    Ok(res) => {
                        debug!("Delete Success: {:?}", res);
                        return Ok(key);
                    }
                    Err(Error::NodeIsNotLeaderError) if retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(Error::ClientRequestCanceledError) if retries < MAX_RETRIES => {
                        retries += 1;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        return Err(Error::GeneralClientError(format!("Error: {:?}", e)));
                    }
                },
                ClientCommands::READ => match self.client.kv().get(kv(key), false).await? {
                    Some(r) => {
                        let v = vk(&r.value);
                        debug!("Success: {:?}", v);
                        return Ok(v);
                    }
                    None => {
                        error!("No entry found for key: {}", key);
                        return Err(Error::GeneralClientError(format!("No entry found for key: {}", key)));
                    }
                },
                ClientCommands::LREAD => match self.client.kv().get(kv(key), true).await {
                    Ok(result) => match result {
                        Some(r) => {
                            let v = vk(&r.value);
                            debug!("Success: {:?}", v);
                            return Ok(v);
                        }
                        None => {
                            error!("No entry found for key: {}", key);
                            return Err(Error::GeneralClientError(format!("No entry found for key: {}", key)));
                        }
                    },
                    Err(Error::NodeIsNotLeaderError) if retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(Error::ClientRequestCanceledError) if retries < MAX_RETRIES => {
                        retries += 1;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        return Err(Error::GeneralClientError(format!("Error: {:?}", e)));
                    }
                },
                _ => {
                    error!("Invalid subcommand");
                    return Err(Error::GeneralClientError("Invalid subcommand".to_string()));
                }
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
            match self.execute_command(ClientCommands::LREAD, key, None).await {
                Ok(v) => assert_eq!(v, expected_value, "Linearizable read failed for key {}!", key),
                Err(status) => {
                    assert!(false);
                }
            }
        }
    }

    pub async fn list_members(
        &self,
        bootstrap_urls: &Vec<String>,
    ) -> Result<Vec<NodeMeta>> {
        let client = match ClientBuilder::new(bootstrap_urls.clone())
            .connect_timeout(Duration::from_secs(3))
            .request_timeout(Duration::from_secs(10))
            .enable_compression(true)
            .build()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("execute_command, {:?}", e);
                return Err(e);
            }
        };
        client.cluster().list_members().await
    }
    pub async fn list_leader_id(
        &self,
        bootstrap_urls: &Vec<String>,
    ) -> Result<u32> {
        let members = self.list_members(bootstrap_urls).await?;
        let mut ids: Vec<u32> = members
            .iter()
            .filter(|meta| meta.role == LEADER)
            .map(|n| n.id)
            .collect();

        Ok(ids.pop().unwrap_or(0))
    }
}
