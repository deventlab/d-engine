fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/generated")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "proto/common.proto",
                "proto/error.proto",
                "proto/raft_election.proto",
                "proto/raft_replication.proto",
                "proto/cluster_management.proto",
                "proto/storage.proto",
                "proto/client_api.proto",
            ],
            &["."],
        )
        .unwrap_or_else(|e| panic!("protobuf compile error: {e}"));

    //autometrics: https://docs.autometrics.dev/rust/adding-version-information
    vergen::EmitBuilder::builder()
        .git_sha(true)
        .git_branch()
        .emit()
        .expect("Unable to generate build info");

    Ok(())
}
