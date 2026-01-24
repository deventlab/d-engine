#![allow(dead_code)]

use std::time::Duration;

use crate::common::ClientCommands;
use crate::common::{self};
use d_engine_client::Client;
use d_engine_client::ClientBuilder;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use d_engine_core::convert::safe_kv_bytes;
use d_engine_core::convert::safe_vk;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::NodeMeta;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;
use tracing::info;

const MAX_RETRIES: u32 = 10;
const RETRY_DELAY_MS: u64 = 100;

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

    /// Refresh cluster membership and connections
    pub async fn refresh(
        &mut self,
        new_endpoints: Option<Vec<String>>,
    ) -> std::result::Result<(), ClientApiError> {
        self.client.refresh(new_endpoints).await
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
                    match self.client.put(safe_kv_bytes(key), safe_kv_bytes(value)).await {
                        Ok(res) => {
                            debug!("Put Success: {:?}", res);
                            return Ok(key);
                        }
                        Err(e) if e.code() == ErrorCode::NotLeader && retries < MAX_RETRIES => {
                            retries += 1;
                            self.refresh_client().await?;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(e)
                            if e.code() == ErrorCode::ConnectionTimeout
                                && retries < MAX_RETRIES =>
                        {
                            retries += 1;

                            sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                        }
                        Err(e) => {
                            error!("ClientCommands::Put, ErrorCode = {:?}", e.code());
                            return Err(e);
                        }
                    }
                }
                ClientCommands::Delete => match self.client.delete(safe_kv_bytes(key)).await {
                    Ok(res) => {
                        debug!("Delete Success: {:?}", res);
                        return Ok(key);
                    }
                    Err(e) if e.code() == ErrorCode::NotLeader && retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) if e.code() == ErrorCode::ConnectionTimeout && retries < MAX_RETRIES => {
                        retries += 1;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        return Err(e);
                    }
                },
                ClientCommands::Read => {
                    match self.client.kv().get_with_policy(safe_kv_bytes(key), None).await? {
                        Some(r) => {
                            let v = safe_vk(&r.value).unwrap();
                            debug!("Success: {:?}", v);
                            return Ok(v);
                        }
                        None => {
                            error!("No entry found for key: {}", key);
                            return Err(ErrorCode::KeyNotExist.into());
                        }
                    }
                }
                ClientCommands::Lread => match self
                    .client
                    .kv()
                    .get_with_policy(
                        safe_kv_bytes(key),
                        Some(ReadConsistencyPolicy::LinearizableRead),
                    )
                    .await
                {
                    Ok(result) => match result {
                        Some(r) => {
                            let v = safe_vk(&r.value).unwrap();
                            debug!("Success: {:?}", v);
                            return Ok(v);
                        }
                        None => {
                            error!("No entry found for key: {}", key);
                            return Err(ErrorCode::KeyNotExist.into());
                        }
                    },
                    Err(e) if e.code() == ErrorCode::NotLeader && retries < MAX_RETRIES => {
                        retries += 1;
                        self.refresh_client().await?;

                        sleep(Duration::from_millis(RETRY_DELAY_MS * 2u64.pow(retries))).await;
                    }
                    Err(e) if e.code() == ErrorCode::ConnectionTimeout && retries < MAX_RETRIES => {
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
        println!("read: {key}",);
        for _ in 0..iterations {
            assert_eq!(
                self.execute_command(ClientCommands::Lread, key, None).await.unwrap(),
                expected_value,
                "Linearizable read failed for key {key}!",
            );
        }
    }

    pub async fn list_members(&self) -> Result<Vec<NodeMeta>, ClientApiError> {
        self.client.cluster().list_members().await
    }
    pub async fn list_leader_id(&self) -> Result<Option<u32>, ClientApiError> {
        self.client.cluster().get_leader_id().await
    }

    /// Test-only: Read from specific node by creating direct gRPC connection
    ///
    /// This method bypasses ClientManager's routing logic and connects directly
    /// to a specified node endpoint to verify data replication.
    ///
    /// # Use Case
    /// - Verify READ_ONLY Learner nodes have complete data replicas
    /// - Test node-specific read capabilities without leader routing
    ///
    /// # Arguments
    /// - `node_endpoint`: Full endpoint URL (e.g., "http://127.0.0.1:50051")
    /// - `key`: Key to read
    /// - `policy`: Read consistency policy
    pub async fn read_from_node(
        node_endpoint: &str,
        key: u64,
        policy: ReadConsistencyPolicy,
    ) -> Result<u64, ClientApiError> {
        use d_engine_proto::client::ClientReadRequest;
        use d_engine_proto::client::raft_client_service_client::RaftClientServiceClient;
        use tonic::transport::Channel;

        // Create direct connection to specified node
        let channel = Channel::from_shared(node_endpoint.to_string())
            .map_err(|_| ClientApiError::from(ErrorCode::InvalidAddress))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|_| ClientApiError::from(ErrorCode::ConnectionTimeout))?;

        let mut client = RaftClientServiceClient::new(channel);

        // Send read request
        let request = ClientReadRequest {
            client_id: 0,
            keys: vec![safe_kv_bytes(key)],
            consistency_policy: Some(policy as i32),
        };

        let response = client
            .handle_client_read(tonic::Request::new(request))
            .await
            .map_err(|_| ClientApiError::from(ErrorCode::General))?;

        let client_response = response.into_inner();
        match client_response.success_result {
            Some(success_result) => match success_result {
                d_engine_proto::client::client_response::SuccessResult::ReadData(read_results) => {
                    let first_result = read_results
                        .results
                        .first()
                        .ok_or(ClientApiError::from(ErrorCode::KeyNotExist))?;
                    Ok(safe_vk(&first_result.value).unwrap())
                }
                _ => Err(ClientApiError::from(ErrorCode::InvalidResponse)),
            },
            None => Err(ClientApiError::from(ErrorCode::General)),
        }
    }
}
