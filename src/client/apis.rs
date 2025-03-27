use std::time::Duration;

use log::{debug, error, warn};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tonic::{
    async_trait,
    codec::CompressionEncoding,
    transport::{Channel, Endpoint},
};

use super::client_config::ClientConfig;
use crate::{
    grpc::rpc_service::{
        rpc_service_client::RpcServiceClient, ClientCommand, ClientProposeRequest,
        ClientReadRequest, ClientRequestError, ClientResponse, ClientResult, MetadataRequest,
        NodeMeta,
    },
    is_follower, is_leader,
    util::{self, address_str},
    Error, Result,
};

pub struct DengineClient {
    id: u32,
    inner: InnerClient,
}

struct InnerClient {
    config: ClientConfig,
    node_metas: Vec<NodeMeta>,
    w_client: RpcServiceClient<Channel>,
    r_clients: Vec<RpcServiceClient<Channel>>,
}
#[async_trait]
pub trait ClientApis {
    async fn read(&mut self, key: Vec<u8>) -> Result<Vec<ClientResult>>;

    ///Linearizable read
    ///
    async fn lread(&mut self, key: Vec<u8>) -> Result<Vec<ClientResult>>;

    async fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    async fn delete(&mut self, key: Vec<u8>) -> Result<()>;
}
impl DengineClient {
    pub async fn new(config: ClientConfig) -> Result<Self> {
        Ok(DengineClient {
            id: util::get_now_as_u32(),
            inner: InnerClient::new(config).await?,
        })
    }
}
#[async_trait]
impl ClientApis for DengineClient {
    async fn read(&mut self, key: Vec<u8>) -> Result<Vec<ClientResult>> {
        //build request
        let mut commands = Vec::new();
        commands.push(ClientCommand::get(key));

        let request = ClientReadRequest {
            client_id: self.id,
            linear: false,
            commands,
        };

        //balance from read clients
        let mut rng = StdRng::from_entropy();
        let i = rng.gen_range(0..self.inner.r_clients.len());

        //send read request
        match self.inner.r_clients[i].handle_client_read(request).await {
            Ok(client_response) => {
                debug!("[:ClientApis:read] response: {:?}", &client_response);

                let res = client_response.get_ref();
                return res.into_read_results();
            }
            Err(status) => {
                error!("[:ClientApis:read] status: {:?}", status);
            }
        }
        return Err(Error::FailedToSendReadRequestError);
    }

    ///Linearizable read
    ///
    async fn lread(&mut self, key: Vec<u8>) -> Result<Vec<ClientResult>> {
        //build request
        let mut commands = Vec::new();
        commands.push(ClientCommand::get(key));

        let request = ClientReadRequest {
            client_id: self.id,
            linear: true,
            commands,
        };

        //send read request
        match self.inner.w_client.handle_client_read(request).await {
            Ok(client_response) => {
                debug!("[:ClientApis:read] response: {:?}", client_response);

                let res = client_response.get_ref();
                return res.into_read_results();
            }
            Err(status) => {
                error!("[:ClientApis:read] status: {:?}", status);
            }
        }
        return Err(Error::FailedToSendReadRequestError);
    }

    async fn write(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        //build request
        let mut commands = Vec::new();
        let client_command_insert = ClientCommand::insert(key, value);
        commands.push(client_command_insert);

        let request = ClientProposeRequest {
            client_id: self.id,
            commands: commands,
        };

        //send write request
        match self.inner.w_client.handle_client_propose(request).await {
            Ok(response) => {
                debug!("[:ClientApis:write] response: {:?}", response);
                match response.get_ref() {
                    ClientResponse {
                        error_code,
                        result: _,
                    } => {
                        if matches!(
                            ClientRequestError::try_from(*error_code)
                                .unwrap_or(ClientRequestError::NoError),
                            ClientRequestError::NoError
                        ) {
                            return Ok(());
                        } else {
                            error!("handle_client_propose error_code:{:?}", error_code);
                        }
                    }
                }
            }
            Err(status) => {
                error!("[:ClientApis:write] status: {:?}", status);
            }
        }
        return Err(Error::FailedToSendWriteRequestError);
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        //build request
        let mut commands = Vec::new();
        let client_command_insert = ClientCommand::delete(key);
        commands.push(client_command_insert);

        let request = ClientProposeRequest {
            client_id: self.id,
            commands: commands,
        };

        //send delete request
        match self.inner.w_client.handle_client_propose(request).await {
            Ok(response) => {
                debug!("[:ClientApis:delete] response: {:?}", response);

                return Ok(());
            }
            Err(status) => {
                error!("[:ClientApis:delete] status: {:?}", status);
            }
        }
        return Err(Error::FailedToSendWriteRequestError);
    }
}

impl InnerClient {
    async fn new(config: ClientConfig) -> Result<Self> {
        let node_metas = Self::load_cluster_metadata(&config).await?;
        println!("node_metas: {:?}", &node_metas);
        let w_client = Self::build_write_client(&node_metas, &config).await?;
        let r_clients = Self::build_read_clients(&node_metas, &config).await?;

        Ok(InnerClient {
            config,
            node_metas,
            w_client,
            r_clients,
        })
    }
    async fn build_write_client(
        cluster_conf: &Vec<NodeMeta>,
        config: &ClientConfig,
    ) -> Result<RpcServiceClient<Channel>> {
        debug!("build_write_client: {:?}", &cluster_conf);

        if let Some(addr) = Self::get_leader_address(cluster_conf).await {
            return Ok(Self::new_client(addr, config).await?);
        }
        return Err(Error::FailedToRetrieveLeaderAddrError);
    }

    async fn build_read_clients(
        note_metas: &Vec<NodeMeta>,
        config: &ClientConfig,
    ) -> Result<Vec<RpcServiceClient<Channel>>> {
        debug!("build_read_clients");
        println!("build_read_clients");

        let mut clients = Vec::new();
        for node_meta in note_metas {
            if is_follower(node_meta.role) || is_leader(node_meta.role) {
                let addr = address_str(&node_meta.ip, node_meta.port as u16);
                debug!("building connection with follower({:?})", &addr);
                match Self::new_client(addr, config).await {
                    Ok(c) => {
                        debug!("success");
                        clients.push(c);
                    }
                    Err(e) => warn!("error to new the read client: {:?}", e),
                }
            }
        }
        return Ok(clients);
    }

    async fn load_cluster_metadata(config: &ClientConfig) -> Result<Vec<NodeMeta>> {
        for i in 0..config.bootstrap_urls.len() {
            match Self::new_client(config.bootstrap_urls[i].clone(), config).await {
                Ok(mut cli) => {
                    match cli
                        .get_cluster_metadata(tonic::Request::new(MetadataRequest {}))
                        .await
                    {
                        Ok(response) => {
                            debug!("[get_leader_address] response: {:?}", response);
                            let res = response.get_ref();
                            return Ok(res.nodes.clone());
                        }
                        Err(status) => {
                            error!("[get_leader_address] response status: {:?}", status);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "load_cluster_metadata: {:?}, {:?}",
                        config.bootstrap_urls[i].clone(),
                        e
                    );
                    continue;
                }
            };
        }

        return Err(Error::FailedToRetrieveLeaderAddrError);
    }

    async fn get_leader_address(cluster_conf: &Vec<NodeMeta>) -> Option<String> {
        for node in cluster_conf {
            if is_leader(node.role) {
                let addr = address_str(&node.ip, node.port as u16);
                debug!("building connection with leader({:?})", &addr);
                return Some(addr);
            }
        }
        return None;
    }

    async fn connect(addr: String, config: &ClientConfig) -> Result<Channel> {
        Endpoint::try_from(addr.clone())?
            .connect_timeout(Duration::from_millis(config.connect_timeout_in_ms))
            .timeout(Duration::from_millis(config.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(
                config.http2_keep_alive_interval_in_secs,
            ))
            .keep_alive_timeout(Duration::from_secs(config.http2_keep_alive_timeout_in_secs))
            .initial_connection_window_size(config.initial_connection_window_size)
            .initial_stream_window_size(config.initial_stream_window_size)
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", &addr, err);
                Error::ConnectError
            })
    }

    async fn new_client(addr: String, config: &ClientConfig) -> Result<RpcServiceClient<Channel>> {
        if let Ok(channel) = Self::connect(addr, config).await {
            let client = RpcServiceClient::new(channel)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
            return Ok(client);
        }

        return Err(Error::ConnectError);
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::sync::oneshot;
    use util::kv;

    use crate::{
        grpc::rpc_service::{ClientRequestError, ClusterMembership},
        test_utils::{self, MockRpcService, MOCK_CLIENT_PORT_BASE},
        FOLLOWER, LEADER,
    };

    use super::*;

    async fn setup_rpc_service(
        key: Vec<u8>,
        value: Vec<u8>,
        port: u64,
        rx: oneshot::Receiver<()>,
        has_leader: bool,
        has_key: bool,
    ) -> Result<SocketAddr> {
        let mut mock_service = MockRpcService::default();
        let result;
        if !has_key {
            result = vec![];
        } else {
            result = vec![ClientResult {
                key: key.clone(),
                value: value.clone(),
            }];
        }

        let response = ClientResponse::read_results(result);

        mock_service.expected_client_read_response = Some(Ok(response));

        let response = ClientResponse::write_success();
        mock_service.expected_client_propose_response = Some(Ok(response));

        let mut cluster_membership = ClusterMembership {
            nodes: vec![
                NodeMeta {
                    id: 2,
                    ip: "127.0.0.1".to_string(),
                    port: port as u32,
                    role: FOLLOWER,
                },
                NodeMeta {
                    id: 3,
                    ip: "127.0.0.1".to_string(),
                    port: port as u32,
                    role: FOLLOWER,
                },
            ],
        };
        if has_leader {
            cluster_membership.nodes.push(NodeMeta {
                id: 1,
                ip: "127.0.0.1".to_string(),
                port: port as u32,
                role: LEADER,
            })
        }
        mock_service.expected_metadata_response = Some(Ok(cluster_membership));
        Ok(test_utils::MockNode::mock_listener(mock_service, port, rx, true).await?)
    }

    /// Case 1: no leader address
    #[tokio::test]
    async fn test_write_read_case1() {
        let port = MOCK_CLIENT_PORT_BASE + 1;
        let (tx, rx) = oneshot::channel::<()>();
        let key = kv(123);
        let value = kv(100);
        let _ = setup_rpc_service(key.clone(), value.clone(), port, rx, false, true).await;
        let cfg = ClientConfig {
            bootstrap_urls: vec![format!("http://127.0.0.1:{}", port)],
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit_per_connection: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            initial_connection_window_size: 12582912,
            initial_stream_window_size: 12582912,
            buffer_size: 65536,
        };
        match DengineClient::new(cfg).await {
            Ok(mut c) => {
                if let Err(_e) = c.write(key.clone(), value.clone()).await {
                    assert!(false);
                }
                if let Err(_e) = c.read(key).await {
                    assert!(true);
                } else {
                    assert!(false);
                }
            }
            Err(e) => {
                assert!(matches!(e, Error::FailedToRetrieveLeaderAddrError));
            }
        };

        tx.send(()).expect("should succeed");
    }

    /// Case 2: has leader address
    // #[traced_test]
    #[tokio::test]
    async fn test_write_read_case2() {
        let port = MOCK_CLIENT_PORT_BASE + 2;
        let (tx, rx) = oneshot::channel::<()>();
        let key = kv(123);
        let none_exist_key = kv(345);
        let value = kv(100);
        let _ = setup_rpc_service(key.clone(), value.clone(), port, rx, true, true).await;
        let cfg = ClientConfig {
            bootstrap_urls: vec![format!("http://127.0.0.1:{}", port)],
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit_per_connection: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            initial_connection_window_size: 12582912,
            initial_stream_window_size: 12582912,
            buffer_size: 65536,
        };
        match DengineClient::new(cfg).await {
            Ok(mut c) => {
                if let Err(_e) = c.write(key.clone(), value.clone()).await {
                    assert!(false);
                }
            }
            Err(e) => {
                eprintln!("{:?}", e);
                assert!(false);
            }
        };

        tx.send(()).expect("should succeed");
    }

    /// Case 3: has leader address, but key doesn't exist
    // #[traced_test]
    #[tokio::test]
    async fn test_write_read_case3() {
        let port = MOCK_CLIENT_PORT_BASE + 3;
        let (tx, rx) = oneshot::channel::<()>();
        let key = kv(123);
        let value = kv(100);
        let _ = setup_rpc_service(key.clone(), value.clone(), port, rx, true, false).await;
        let cfg = ClientConfig {
            bootstrap_urls: vec![format!("http://127.0.0.1:{}", port)],
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit_per_connection: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            initial_connection_window_size: 12582912,
            initial_stream_window_size: 12582912,
            buffer_size: 65536,
        };
        match DengineClient::new(cfg).await {
            Ok(mut c) => {
                if let Ok(_v) = c.delete(key.clone()).await {
                    assert!(true);
                } else {
                    assert!(false);
                }
                if let Ok(v) = c.read(key).await {
                    assert_eq!(v.len(), 0);
                } else {
                    assert!(false);
                }
            }
            Err(e) => {
                eprintln!("{:?}", e);
                assert!(false);
            }
        };

        tx.send(()).expect("should succeed");
    }
}
