use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use super::Client;
use super::ClientApiError;
use super::ClientConfig;
use super::ClientInner;
use super::ClusterClient;
use super::ConnectionPool;
use super::KvClient;

/// Configurable builder for [`Client`] instances
///
/// Implements the **builder pattern** for constructing clients with
/// customized connection parameters and timeouts.
///
/// # Typical Usage Flow
/// 1. Create with `ClientBuilder::new()`
/// 2. Chain configuration methods
/// 3. Finalize with `.build()`
///
/// # Default Configuration
/// - Compression: Enabled
/// - Connect Timeout: 1s
/// - Request Timeout: 3s
pub struct ClientBuilder {
    config: ClientConfig,
    endpoints: Vec<String>,
}

impl ClientBuilder {
    /// Create a new builder with default config and specified endpoints
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            config: ClientConfig::default(),
            endpoints,
        }
    }

    /// Set connection timeout (default: 1s)
    pub fn connect_timeout(
        mut self,
        timeout: Duration,
    ) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set request timeout (default: 3s)
    pub fn request_timeout(
        mut self,
        timeout: Duration,
    ) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Enable/disable compression (default: enabled)
    pub fn enable_compression(
        mut self,
        enable: bool,
    ) -> Self {
        self.config.enable_compression = enable;
        self
    }

    /// Completely replaces the default configuration
    ///
    /// # Warning: Configuration Override
    /// This will discard all previous settings configured through individual
    /// methods like [`connect_timeout`](ClientBuilder::connect_timeout) or
    /// [`enable_compression`](ClientBuilder::enable_compression).
    ///
    /// # Usage Guidance
    /// Choose **either**:
    /// - Use granular configuration methods (recommended for most cases)
    /// - Use this method to provide a full configuration object
    ///
    /// # Example: Full Configuration
    /// ```no_run
    /// use d_engine_client::ClientBuilder;
    /// use d_engine_client::ClientConfig;
    /// use std::time::Duration;
    ///
    /// let custom_config = ClientConfig {
    ///     connect_timeout: Duration::from_secs(2),
    ///     request_timeout: Duration::from_secs(5),
    ///     ..ClientConfig::default()
    /// };
    ///
    /// let builder = ClientBuilder::new(vec!["http://node1:9081".into()])
    ///     .set_config(custom_config);
    /// ```
    pub fn set_config(
        mut self,
        config: ClientConfig,
    ) -> Self {
        self.config = config;
        self
    }

    /// Build the client with current configuration
    pub async fn build(self) -> std::result::Result<Client, ClientApiError> {
        let pool = ConnectionPool::create(self.endpoints.clone(), self.config.clone()).await?;
        let inner = Arc::new(ArcSwap::from_pointee(ClientInner {
            pool,
            client_id: self.config.id,
            config: self.config,
            endpoints: self.endpoints,
        }));

        Ok(Client {
            kv: KvClient::new(inner.clone()),
            cluster: ClusterClient::new(inner.clone()),
            inner,
        })
    }
}
