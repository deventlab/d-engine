use std::time::Duration;

use log::error;
#[cfg(test)]
use mockall::automock;
#[cfg(test)]
use mockall::predicate::*;
use tonic::async_trait;
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::HealthCheckResponse;

use crate::NetworkConfig;
use crate::NetworkError;
use crate::Result;

#[cfg_attr(test, automock)]
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
    async fn connect(
        addr: &str,
        settings: NetworkConfig,
    ) -> Result<Self> {
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|_| NetworkError::InvalidURI(addr.into()))?
            .connect_timeout(Duration::from_millis(settings.connect_timeout_in_ms))
            .timeout(Duration::from_millis(settings.request_timeout_in_ms))
            .tcp_keepalive(Some(Duration::from_secs(settings.tcp_keepalive_in_secs)))
            .http2_keep_alive_interval(Duration::from_secs(settings.http2_keep_alive_interval_in_secs))
            .keep_alive_timeout(Duration::from_secs(settings.http2_keep_alive_timeout_in_secs))
            .connect()
            .await
            .map_err(|err| {
                error!("connect to {} failed: {}", addr, err);
                eprintln!("{:?}", err);
                NetworkError::ConnectError
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
                NetworkError::ConnectError
            })?
            .into_inner();

        if response.status == ServingStatus::Serving as i32 {
            return Ok(());
        } else {
            return Err(NetworkError::ServiceUnavailable("RPC service is not ready yet".to_string()).into());
        }
    }
}
