mod builder;
mod client;
mod cluster;
mod config;
mod kv;
mod pool;

pub use builder::*;
pub use client::*;
pub use cluster::*;
pub use config::*;
pub use kv::*;
use log::error;
pub use pool::*;

#[cfg(test)]
mod pool_test;

//---

use crate::{
    grpc::rpc_service::{
        client_command, client_response, ClientCommand, ClientRequestError, ClientResponse,
        ClientResult, ReadResults,
    },
    Error, Result,
};

impl ClientCommand {
    pub fn get(key: impl AsRef<[u8]>) -> Self {
        Self {
            command: Some(client_command::Command::Get(key.as_ref().to_vec())),
        }
    }

    pub fn insert(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        let insert_cmd = client_command::Insert {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        };
        Self {
            command: Some(client_command::Command::Insert(insert_cmd)),
        }
    }

    pub fn delete(key: impl AsRef<[u8]>) -> Self {
        Self {
            command: Some(client_command::Command::Delete(key.as_ref().to_vec())),
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

    pub fn into_read_results(&self) -> Result<Vec<Option<ClientResult>>> {
        self.validate_error()?;
        match &self.result {
            Some(client_response::Result::ReadResults(read_results)) => read_results
                .results
                .clone()
                .into_iter()
                .map(|item| {
                    Ok(Some(ClientResult {
                        key: item.key.to_vec(),
                        value: item.value.to_vec(),
                    }))
                })
                .collect(),
            _ => {
                error!("Invalid response type for read operation");
                Err(Error::InvalidResponseType)
            }
        }
    }

    fn validate_error(&self) -> Result<()> {
        match ClientRequestError::try_from(self.error_code).unwrap_or(ClientRequestError::NoError) {
            ClientRequestError::NoError => Ok(()),
            e => Err(e.into()),
        }
    }
}
