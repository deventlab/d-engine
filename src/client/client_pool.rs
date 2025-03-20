use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::grpc::rpc_service::rpc_service_client::RpcServiceClient;
use crate::{ChannelPool, Error, Result};

pub struct ClientPool {
    channel_pool: ChannelPool,
    max_clients: usize,
    clients: RwLock<VecDeque<RpcServiceClient<Channel>>>,
}

impl ClientPool {
    pub fn new(channel_pool: ChannelPool, max_clients: usize) -> Self {
        ClientPool {
            channel_pool,
            max_clients,
            clients: RwLock::new(VecDeque::new()),
        }
    }

    pub fn init_pool(&self) -> &Self {
        let mut clients = self.clients.write();
        while clients.len() < self.max_clients {
            if let Ok(cli) = self.channel_pool.acquire() {
                let client = RpcServiceClient::new(cli.clone())
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);

                clients.push_back(client.clone());
            }
        }
        self
    }
    pub fn take(&self, index: usize) -> Result<RpcServiceClient<Channel>> {
        let clients = self.clients.read();
        if let Some(client) = clients.get(index) {
            return Ok(client.clone());
        }
        return Err(Error::ChannelError(
            "Channel pool limit reached".to_string(),
        ));
    }
}
