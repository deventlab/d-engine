fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/rpc_service.proto"], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    //autometrics: https://docs.autometrics.dev/rust/adding-version-information
    vergen::EmitBuilder::builder()
        .git_sha(true)
        .git_branch()
        .emit()
        .expect("Unable to generate build info");

    Ok(())
}
