fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/generated")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "proto/common.proto",
                "proto/error.proto",
                "proto/server/election.proto",
                "proto/server/replication.proto",
                "proto/server/cluster.proto",
                "proto/server/storage.proto",
                "proto/client/client_api.proto",
            ],
            &["."],
        )
        .unwrap_or_else(|e| panic!("protobuf compile error: {e}"));

    vergen::EmitBuilder::builder()
        .git_sha(true)
        .git_branch()
        .emit()
        .expect("Unable to generate build info");

    Ok(())
}
