use std::time::Duration;

use d_engine_core::NetworkConfig;
use d_engine_core::NetworkError;
use d_engine_core::Result;
#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;
#[cfg(any(test, feature = "test-utils"))]
use mockall::predicate::*;
use tonic::async_trait;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::HealthCheckResponse;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tracing::error;

use crate::utils::net::address_str;

#[cfg_attr(any(test, feature = "test-utils"), automock)]
#[async_trait]
pub(crate) trait HealthCheckerApis {
    async fn check_peer_is_ready(
        peer_addr: String,
        settings: NetworkConfig,
        service: String,
    ) -> Result<()>;
}
pub(crate) struct HealthChecker {
    client: HealthClient<Channel>,
}

impl HealthChecker {
    #[allow(dead_code)]
    async fn connect(
        addr: &str,
        settings: NetworkConfig,
    ) -> Result<Self> {
        let address = address_str(addr);
        let channel = Channel::from_shared(address.clone())
            .map_err(|_| NetworkError::InvalidURI(addr.into()))?
            .connect_timeout(Duration::from_millis(
                settings.control.connect_timeout_in_ms,
            ))
            .timeout(Duration::from_millis(
                settings.control.request_timeout_in_ms,
            ))
            .tcp_keepalive(Some(Duration::from_secs(
                settings.control.tcp_keepalive_in_secs,
            )))
            .http2_keep_alive_interval(Duration::from_secs(
                settings.control.http2_keep_alive_interval_in_secs,
            ))
            .keep_alive_timeout(Duration::from_secs(
                settings.control.http2_keep_alive_timeout_in_secs,
            ))
            .connect()
            .await
            .map_err(|err| {
                // Use debug level during startup - connection failures are expected
                // when peers are still starting up
                tracing::debug!("Failed to connect to {}: {}", address, err);
                NetworkError::ConnectError(err.to_string())
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
    async fn check_peer_is_ready(
        peer_addr: String,
        settings: NetworkConfig,
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
                NetworkError::ConnectError(err.to_string())
            })?
            .into_inner();

        if response.status == ServingStatus::Serving as i32 {
            return Ok(());
        } else {
            return Err(NetworkError::ServiceUnavailable(
                "RPC service is not ready yet".to_string(),
            )
            .into());
        }
    }
}
