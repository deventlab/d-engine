use std::io::BufRead;
use std::io::Write;
use std::time::Duration;

use d_engine_server::DefaultEmbeddedEngine;

use crate::common::get_available_ports;

const CHILD_ENV_VAR: &str = "D_ENGINE_TEST_LOCK_CHILD";
const DATA_DIR_ENV_VAR: &str = "D_ENGINE_TEST_DATA_DIR";
const READY_MARKER: &str = "LOCK_ACQUIRED";

/// A node process holding the data_dir lock crashes (SIGKILL, no clean
/// shutdown). The OS must release the flock on process exit — otherwise a
/// crashed node would permanently block itself (or any replacement) from
/// ever restarting against the same data_dir.
#[tokio::test]
async fn test_lock_released_after_process_crash() {
    // Child branch: re-invocation of this same test binary, acting as the
    // "process that crashes". Selected via env var set by the parent below.
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        let data_dir = std::env::var(DATA_DIR_ENV_VAR).expect("parent sets data_dir");
        let _engine = DefaultEmbeddedEngine::start(&data_dir)
            .await
            .expect("child should acquire the data_dir lock");

        println!("{READY_MARKER}");
        // Don't rely on LineWriter's current auto-flush-on-'\n' behavior for
        // piped (non-tty) stdout — it's an implementation detail under
        // active discussion (rust-lang/rust#60673), not a stable contract.
        std::io::stdout().flush().ok();

        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }

    // Parent branch: spawn the child, wait for it to hold the lock, kill -9
    // it, then prove the same data_dir can be started again.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut child_ports = get_available_ports(1).await;
    child_ports.release_listeners();
    let child_addr = format!("127.0.0.1:{}", child_ports[0]);

    let exe = std::env::current_exe().unwrap();
    // Full module path, not the bare fn name: this binary aggregates every
    // tests/*/*.rs file via `mod` in integration_test.rs, so a bare name
    // would match zero tests instead of exactly one. `module_path!()` includes
    // this test binary's own crate-root segment ("integration_test::"), which
    // libtest's own test names never include — strip it before matching.
    let raw_module_path = module_path!();
    let module_path = raw_module_path
        .split_once("::")
        .map(|(_, rest)| rest)
        .unwrap_or(raw_module_path);
    let filter = format!("{module_path}::test_lock_released_after_process_crash");

    let mut child = std::process::Command::new(&exe)
        .args([filter.as_str(), "--exact", "--nocapture"])
        .env(CHILD_ENV_VAR, "1")
        .env(DATA_DIR_ENV_VAR, temp_dir.path())
        .env("RAFT__CLUSTER__NODE_ID", "1")
        .env("RAFT__CLUSTER__LISTEN_ADDRESS", &child_addr)
        .stdout(std::process::Stdio::piped())
        // Own pipe instead of inheriting the parent's stderr fd: without this,
        // the child's tracing output can land on that shared fd after this test
        // function returns (child dies a beat later than `child.wait()` sees),
        // which nextest flags as "leaky" output on the parent test.
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn child process");
    // Drain and discard — we only need the fd detached, not the content.
    let child_stderr = child.stderr.take().expect("child stderr piped");
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(child_stderr);
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            line.clear();
        }
    });

    wait_for_line(&mut child, READY_MARKER, Duration::from_secs(10));

    child.kill().expect("SIGKILL child"); // std::process::Child::kill() == SIGKILL on Unix
    child.wait().expect("reap child");

    // Same data_dir, fresh port (avoids relying on TIME_WAIT semantics of
    // the killed child's listener) — data_dir is what's under test here.
    let mut restart_ports = get_available_ports(1).await;
    restart_ports.release_listeners();
    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var(
            "RAFT__CLUSTER__LISTEN_ADDRESS",
            format!("127.0.0.1:{}", restart_ports[0]),
        );
    }
    let result = DefaultEmbeddedEngine::start(temp_dir.path()).await;
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }

    assert!(
        result.is_ok(),
        "data_dir lock must release after the holder crashes: {:?}",
        result.err()
    );
    result.unwrap().stop().await.ok();
}

/// Blocks until `expected` appears on the child's stdout, or times out.
/// On timeout, actively kills the child so a failed assertion never leaves
/// a hung process or zombie behind in CI.
fn wait_for_line(
    child: &mut std::process::Child,
    expected: &str,
    timeout: Duration,
) {
    let stdout = child.stdout.take().expect("child stdout piped");
    let (tx, rx) = std::sync::mpsc::channel();
    let expected_owned = expected.to_string();

    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            if line.trim() == expected_owned {
                let _ = tx.send(());
                return;
            }
            line.clear();
        }
    });

    if rx.recv_timeout(timeout).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("child never reported '{expected}' within {timeout:?}");
    }
}
