use crate::{client_config::ClientConfig, Error, Result};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

pub struct ChannelPool {
    endpoint: String,
    cfg: ClientConfig,
    max_channels: usize,
    channels: Mutex<VecDeque<Channel>>,
}

impl ChannelPool {
    pub fn new(endpoint: String, max_channels: usize, cfg: ClientConfig) -> Self {
        ChannelPool {
            endpoint,
            max_channels,
            cfg,
            channels: Mutex::new(VecDeque::new()),
        }
    }

    pub fn acquire(&self) -> Result<Channel> {
        // Try to acquire a channel from the pool
        let mut channels = self.channels.lock();
        if let Some(channel) = channels.pop_front() {
            Ok(channel)
        } else {
            // If no channel available, create a new one if within max limit
            if channels.len() < self.max_channels {
                let new_channel = self.connect(self.endpoint.clone())?;
                // Add the new channel to the pool
                channels.push_back(new_channel.clone());

                Ok(new_channel)
            } else {
                Err(Error::ChannelError(
                    "Channel pool limit reached".to_string(),
                ))
            }
        }
    }

    pub fn release(&self, channel: Channel) {
        // Return the channel back to the pool (if possible)
        self.channels.lock().push_back(channel);
    }

    fn connect(&self, address_str: String) -> Result<Channel> {
        match Endpoint::try_from(address_str) {
            Ok(mut endpoint) => {
                // Configure the endpoint with necessary settings
                endpoint = endpoint
                    // .concurrency_limit(RPC_CLIENT_CONCURRENCY_LIMIT_PER_CONNECTION) // Set maximum concurrency
                    // .tcp_keepalive(Some(Duration::from_secs(RPC_CLIENT_TCP_KEEP_ALIVE_SEC))) // TCP keepalive
                    // .initial_connection_window_size(RPC_CLIENT_INITIAL_CONNECTION_WINDOW_SIZE_BYTES) // 5MB initial connection window
                    // .initial_stream_window_size(RPC_CLIENT_INTIAL_STREAM_WINDOWN_SIZE_BYTES) // 2MB initial stream window
                    .timeout(Duration::from_millis(self.cfg.connect_timeout_in_ms)); // Connection timeout

                return Ok(endpoint.connect_lazy());
            }
            Err(e) => {
                eprintln!("channel pool > try_from failed with error: {:?}", e);
            }
        }

        Err(Error::ConnectError)
    }
}
