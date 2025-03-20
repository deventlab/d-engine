use std::time::Duration;

use log::{debug, error, warn};
use tokio::time::timeout;
use tonic::{transport::Channel, Code};

use crate::{
    grpc::rpc_service::{
        rpc_service_client::RpcServiceClient, ClientProposeRequest, ClientRequestError,
        ClientResponse, MetadataRequest,
    },
    is_leader, Error, Result,
};

pub struct NewClient;

impl NewClient {
    pub async fn get_leader_address(mut client: RpcServiceClient<Channel>) -> Option<String> {
        let request = MetadataRequest {};
        debug!("request: {:?}", request);
        match client
            .get_cluster_metadata(tonic::Request::new(request))
            .await
        {
            Ok(response) => {
                debug!("[get_leader_address] response: {:?}", response);

                // println!("resquest [peer({:?})] client response: {:?}", dst, response);
                let res = response.get_ref();

                for n in &res.nodes {
                    if is_leader(n.role) {
                        return Some(format!("http://{}:{}", n.ip, n.port));
                    }
                }

                return None;
            }
            Err(status) => {
                eprintln!("status: {:?}", status);
            }
        }

        return None;
    }

    pub async fn send_write_request(
        mut client: RpcServiceClient<Channel>,
        request: ClientProposeRequest,
    ) -> Result<()> {
        debug!("request: {:?}", request);

        match timeout(
            Duration::from_millis(10000),
            client.handle_client_propose(tonic::Request::new(request.clone())),
        )
        .await
        {
            Ok(result) => {
                match result {
                    Ok(response) => {
                        // println!("client response: {:?}", response);
                        return NewClient::process_write_response(&request, response.get_ref())
                            .await;
                    }
                    Err(status) => {
                        if status.code() == Code::Unavailable {
                            eprintln!("Service is unavailable.");
                        } else {
                            eprintln!("Received different status: {}", status);
                        }
                        return Err(Error::GeneralServerError(format!("{:?}", status)));
                    }
                }
            }
            Err(_) => {
                eprintln!("timeout error!");
                return Err(Error::RpcTimeout);
            }
        }
    }

    async fn process_write_response(
        request: &ClientProposeRequest,
        response: &ClientResponse,
    ) -> Result<()> {
        match ClientRequestError::try_from(response.error_code) {
            Ok(ClientRequestError::NoError) => {
                debug!("result: {:?}", response.result);
                return Ok(());
            }
            Ok(ClientRequestError::CommitNotConfirmed) => {
                eprintln!("{:?} request not commited yet.", request);
                return Err(Error::RequestNotCommittedYet);
            }
            Ok(ClientRequestError::ServerError) => {
                eprintln!(
                    "Error Code: {:?}, request receives GeneralServerError.",
                    response.error_code
                );
                return Err(Error::GeneralServerError(format!(
                    "{:?} request receives GeneralServerError.",
                    request
                )));
            }
            Ok(ClientRequestError::NotLeader) => {
                return Err(Error::NodeIsNotLeaderError);
            }
            Ok(ClientRequestError::ServerTimeout) => {
                eprintln!("client request to server timeouts.");
                return Err(Error::ClientRequestTimeoutError(format!(
                    "client request to server timeouts."
                )));
            }
            Ok(ClientRequestError::Unknown) => {
                warn!("should be a bug while receive unknown client request error.");
                return Err(Error::ServerError);
            }
            Err(e) => {
                error!(
                    "failed on ClientRequestError::try_from(error_code): {:?}",
                    e
                );
                return Err(Error::ChannelError(format!(
                    "failed on ClientRequestError::try_from(error_code): {:?}",
                    e
                )));
            }
        }
    }
}
