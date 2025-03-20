use crate::{Error, Result, RpcConnectionSettings};
use log::error;
use std::time::Duration;
use tonic::{async_trait, transport::Channel};
use tonic_health::pb::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
    HealthCheckResponse,
};

#[cfg(test)]
use mockall::{automock, predicate::*};

#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait HealthCheckerApis {
    async fn check_peer_is_ready(
        peer_addr: String,
        settings: RpcConnectionSettings,
        service: String,
    ) -> Result<()>;
}
pub(crate) struct HealthChecker {
    client: HealthClient<Channel>,
}

impl HealthChecker {
    async fn connect(addr: &str, settings: RpcConnectionSettings) -> Result<Self> {
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|_| Error::InvalidURI(addr.into()))?
            .connect_timeout(Duration::from_millis(settings.connect_timeout_in_ms))
            .timeout(Duration::from_millis(settings.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(settings.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(
                settings.http2_keep_alive_interval_in_secs,
            ))
            .keep_alive_timeout(Duration::from_secs(
                settings.http2_keep_alive_timeout_in_secs,
            ))
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", addr, err);
                eprintln!("{:?}", err);
                Error::ConnectError
            })?;

        let client = HealthClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }
}

#[async_trait]
impl HealthCheckerApis for HealthChecker {
    /// peer_addr: "http://[::1]:50051"
    ///
    async fn check_peer_is_ready(
        peer_addr: String,
        settings: RpcConnectionSettings,
        service: String,
    ) -> Result<()> {
        let mut checker = Self::connect(&peer_addr, settings).await?;

        let request = tonic::Request::new(HealthCheckRequest { service });

        let response: HealthCheckResponse = checker
            .client
            .check(request)
            .await
            .map_err(|err| {
                error!("client.check to {} failed: {}", peer_addr, err);
                Error::ConnectError
            })?
            .into_inner();

        if response.status == ServingStatus::Serving as i32 {
            return Ok(());
        } else {
            return Err(Error::ServerIsNotReadyError);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{self, MockNode, MockRpcService};
    use tokio::{net::TcpStream, sync::oneshot};

    use super::*;

    /// Case 1: server is not ready
    #[tokio::test]
    async fn test_check_peer_is_ready_case1() {
        let port = crate::test_utils::MOCK_HEALTHCHECK_PORT_BASE + 1;
        let (tx, rx) = oneshot::channel::<()>();
        let is_ready = false;
        let mock_service = MockRpcService::default();
        let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, is_ready).await
        {
            Ok(a) => a,
            Err(_) => {
                assert!(false);
                return;
            }
        };

        let peer_addr = addr.to_string();
        loop {
            if TcpStream::connect(&peer_addr).await.is_ok() {
                println!("Node is ready!");
                break;
            } else {
                eprintln!("Node not ready, retrying...");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        let mut settings = RpcConnectionSettings::default();
        settings.connect_timeout_in_ms = 100;
        settings.request_timeout_in_ms = 100;
        settings.concurrency_limit_per_connection = 8192;
        settings.tcp_keepalive_in_secs = 3600;
        settings.http2_keep_alive_interval_in_secs = 300;
        settings.http2_keep_alive_timeout_in_secs = 20;
        settings.max_frame_size = 12582912;
        settings.initial_connection_window_size = 12582912;
        settings.initial_stream_window_size = 12582912;
        settings.buffer_size = 65536;

        let r = HealthChecker::check_peer_is_ready(
            MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
            settings,
            "rpc_service.RpcService".to_string(),
        )
        .await;
        assert!(r.is_err());
        tx.send(()).expect("should succeed");
    }

    /// Case 2: server is ready
    #[tokio::test]
    async fn test_check_peer_is_ready_case2() {
        let port = crate::test_utils::MOCK_HEALTHCHECK_PORT_BASE + 2;
        let (tx, rx) = oneshot::channel::<()>();
        let is_ready = true;
        let mock_service = MockRpcService::default();
        let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, is_ready).await
        {
            Ok(a) => a,
            Err(_) => {
                assert!(false);
                return;
            }
        };
        let peer_addr = addr.to_string();
        loop {
            if TcpStream::connect(&peer_addr).await.is_ok() {
                println!("Node is ready!");
                break;
            } else {
                eprintln!("Node not ready, retrying...");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
        let mut settings = RpcConnectionSettings::default();
        settings.connect_timeout_in_ms = 100;
        settings.request_timeout_in_ms = 100;
        settings.concurrency_limit_per_connection = 8192;
        settings.tcp_keepalive_in_secs = 3600;
        settings.http2_keep_alive_interval_in_secs = 300;
        settings.http2_keep_alive_timeout_in_secs = 20;
        settings.max_frame_size = 12582912;
        settings.initial_connection_window_size = 12582912;
        settings.initial_stream_window_size = 12582912;
        settings.buffer_size = 65536;

        let r = HealthChecker::check_peer_is_ready(
            MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
            settings,
            "rpc_service.RpcService".to_string(),
        )
        .await;
        assert!(r.is_ok());
        tx.send(()).expect("should succeed");
    }
}
