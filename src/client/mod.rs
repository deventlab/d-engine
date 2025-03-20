mod channel_pool;
mod client_pool;
mod new_client;
pub use channel_pool::*;
pub use client_pool::*;
pub use new_client::*;
mod apis;

pub use apis::*;

use crate::{
    grpc::rpc_service::{
        client_command, client_response, ClientCommand, ClientRequestError, ClientResponse,
        ClientResult, ReadResults,
    },
    Error, Result,
};
pub mod client_config;

impl ClientCommand {
    pub fn get(key: Vec<u8>) -> Self {
        Self {
            command: Some(client_command::Command::Get(key)),
        }
    }

    pub fn insert(key: Vec<u8>, value: Vec<u8>) -> Self {
        let insert_cmd = client_command::Insert { key, value };
        Self {
            command: Some(client_command::Command::Insert(insert_cmd)),
        }
    }

    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            command: Some(client_command::Command::Delete(key)),
        }
    }

    pub fn no_op() -> Self {
        Self {
            command: Some(client_command::Command::NoOp(true)),
        }
    }
}
impl ClientResponse {
    pub fn write_success() -> Self {
        Self {
            error_code: ClientRequestError::NoError as i32,
            result: Some(client_response::Result::WriteResult(true)),
        }
    }

    pub fn write_error(error: Error) -> Self {
        Self {
            error_code: <ClientRequestError as Into<i32>>::into(ClientRequestError::from(error)),
            result: None,
        }
    }

    pub fn read_results(results: Vec<ClientResult>) -> Self {
        Self {
            error_code: ClientRequestError::NoError as i32,
            result: Some(client_response::Result::ReadResults(ReadResults {
                results,
            })),
        }
    }

    /// works for both write and read
    pub fn error(error: ClientRequestError) -> Self {
        Self {
            error_code: error as i32,
            result: None,
        }
    }

    pub fn into_write_result(&self) -> Result<bool> {
        self.validate_error()?;
        Ok(match self.result {
            Some(client_response::Result::WriteResult(success)) => success,
            _ => false,
        })
    }

    pub fn into_read_results(&self) -> Result<Vec<ClientResult>> {
        self.validate_error()?;
        Ok(match &self.result {
            Some(client_response::Result::ReadResults(r)) => r.results.clone(),
            _ => Vec::new(),
        })
    }

    fn validate_error(&self) -> Result<()> {
        match ClientRequestError::try_from(self.error_code).unwrap_or(ClientRequestError::NoError) {
            ClientRequestError::NoError => Ok(()),
            e => Err(e.into()),
        }
    }
}
