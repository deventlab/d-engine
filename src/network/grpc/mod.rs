//! gRPC transport implementation with deadline propagation
//!
//! This submodule implements timeout-aware gRPC communication using tower and
//! tonic. All RPC methods enforce server-side deadlines based on the configured
//! timeout values.

// Protobuf GRPC service introduction
// -----------------------------------------------------------------------------

mod backgroup_snapshot_transfer;
mod restartable_stream;
pub(crate) use backgroup_snapshot_transfer::*;
pub(crate) use restartable_stream::*;
mod grpc_raft_service;
pub(crate) mod grpc_transport;

#[cfg(test)]
mod grpc_raft_service_test;

#[cfg(test)]
mod grpc_transport_test;

#[cfg(test)]
mod backgroup_snapshot_transfer_test;

//-------------------------------------------------------------------------------
// Start RPC Server
use crate::proto::client::raft_client_service_server::RaftClientServiceServer;
use crate::proto::cluster::cluster_management_service_server::ClusterManagementServiceServer;
use crate::proto::election::raft_election_service_server::RaftElectionServiceServer;
use crate::proto::replication::raft_replication_service_server::RaftReplicationServiceServer;
use crate::proto::storage::snapshot_service_server::SnapshotServiceServer;
use crate::Node;
use crate::RaftNodeConfig;
use crate::Result;
use crate::SystemError;
use crate::TlsConfig;
use crate::TypeConfig;
use futures::FutureExt;
use rcgen::generate_simple_self_signed;
use rcgen::CertifiedKey;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tonic::codec::CompressionEncoding;
use tonic::transport::Certificate;
use tonic::transport::Identity;
use tonic::transport::ServerTlsConfig;
use tonic_health::server::health_reporter;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

/// RPC server works for RAFT protocol
/// It mainly listens on two request: Vote RPC Request and Append Entries RPC
/// Request The server itself knows its peers.
pub(crate) async fn start_rpc_server<T>(
    node: Arc<Node<T>>,
    listen_address: SocketAddr,
    config: RaftNodeConfig,
    mut shutdown_signal: watch::Receiver<()>,
) -> Result<()>
where
    T: TypeConfig,
{
    // Create a HealthReporter to manage the health status
    let (mut health_reporter, health_service) = health_reporter();

    // Set the initial health status to SERVING
    health_reporter.set_serving::<RaftClientServiceServer<Node<T>>>().await;
    health_reporter
        .set_serving::<RaftElectionServiceServer<Node<T>>>()
        .await;
    health_reporter
        .set_serving::<RaftReplicationServiceServer<Node<T>>>()
        .await;
    health_reporter
        .set_serving::<ClusterManagementServiceServer<Node<T>>>()
        .await;
    health_reporter.set_serving::<SnapshotServiceServer<Node<T>>>().await;

    // Use control plane configuration for base parameters
    // Rationale: Election RPCs need low latency and high reliability
    let control_config = &config.network.control;

    // Use data plane configuration for stream/flow control parameters
    // Rationale: AppendEntries needs higher throughput when large logs are sent
    let data_config = &config.network.data;

    let mut server_builder = tonic::transport::Server::builder()
        // Control-plane parameters for core RPC operations
        // .timeout(Duration::from_millis(control_config.request_timeout_in_ms))
        .concurrency_limit_per_connection(control_config.concurrency_limit)
        // .max_concurrent_streams(control_config.max_concurrent_streams)
        .tcp_keepalive(Some(Duration::from_secs(control_config.tcp_keepalive_in_secs)))
        .http2_keepalive_interval(Some(Duration::from_secs(
            control_config.http2_keep_alive_interval_in_secs,
        )))
        .http2_keepalive_timeout(Some(Duration::from_secs(
            control_config.http2_keep_alive_timeout_in_secs,
        )))
        // Data-plane parameters for stream performance
        .initial_stream_window_size(data_config.stream_window_size)
        .initial_connection_window_size(data_config.connection_window_size)
        .http2_adaptive_window(Some(data_config.adaptive_window))
        // .max_frame_size(Some(data_config.max_frame_size))
        // Common TCP parameters
        .tcp_nodelay(config.network.tcp_nodelay);

    if config.tls.enable_tls {
        if config.tls.generate_self_signed_certificates {
            if Path::new(&config.tls.certificate_authority_root_path).exists() {
                warn!("Certificate authority root already exists, remove the file if you want to generate new certificates. Skipping self signed certificates generation.");
            } else {
                info!("Generating self signed certificates");
                generate_self_signed_certificates(config.tls.clone());
            }
        }
        let cert = std::fs::read_to_string(config.tls.server_certificate_path.clone())
            .expect("error, failed to read server certificat");
        let key = std::fs::read_to_string(config.tls.server_private_key_path.clone())
            .expect("error, failed to read server private key");
        let server_identity = Identity::from_pem(cert, key);
        let tls = ServerTlsConfig::new().identity(server_identity);
        if config.tls.enable_mtls {
            let client_ca_cert = std::fs::read_to_string(config.tls.client_certificate_authority_root_path.clone())
                .expect("error, failed to read client certificate authority root");
            let client_ca_cert = Certificate::from_pem(client_ca_cert);
            let tls = tls.client_ca_root(client_ca_cert);
            server_builder = server_builder.tls_config(tls).expect("error, failed to setup mTLS");
            info!("gRPC mTLS enabled");
        } else {
            server_builder = server_builder.tls_config(tls).expect("error, failed to setup TLS");
            info!("gRPC TLS enabled");
        }
    }

    if let Err(e) = server_builder
        .add_service(health_service)
        .add_service(
            RaftClientServiceServer::from_arc(node.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            RaftElectionServiceServer::from_arc(node.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            RaftReplicationServiceServer::from_arc(node.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            ClusterManagementServiceServer::from_arc(node.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            SnapshotServiceServer::from_arc(node)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve_with_shutdown(
            // SocketAddr::from_str(&listen_address).map_err(|e| Error::AddrParseError(e))?,
            listen_address,
            shutdown_signal.changed().map(|_s| {
                warn!("Stopping RPC server. {}", listen_address);
            }),
        )
        .await
    {
        error!("error to start internal rpc server :{:?}.", e);
        return Err(SystemError::ServerUnavailable.into());
    }
    debug!("rpc service finished!");
    Ok(())
}

// Implement the certificate generation function
fn generate_self_signed_certificates(config: TlsConfig) {
    // Example using rcgen to generate self-signed certificates
    let subject_alt_names = vec!["localhost".to_string()];
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(subject_alt_names).expect("Certificate generation failed");

    // Write certificate and private key to files
    std::fs::write(&config.server_certificate_path, cert.pem()).expect("Should succeed to write server certificate");
    std::fs::write(&config.server_private_key_path, key_pair.serialize_pem())
        .expect("Should succeed to write server private key");
}
