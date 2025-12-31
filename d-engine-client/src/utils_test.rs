use super::*;

#[test]
fn test_address_str_no_scheme() {
    assert_eq!(address_str("127.0.0.1:9000"), "http://127.0.0.1:9000");
    assert_eq!(address_str("node1:9000"), "http://node1:9000");
    assert_eq!(address_str("localhost:8080"), "http://localhost:8080");
}

#[test]
fn test_address_str_preserves_http() {
    assert_eq!(
        address_str("http://127.0.0.1:9000"),
        "http://127.0.0.1:9000"
    );
    assert_eq!(address_str("http://node1:9000"), "http://node1:9000");
}

#[test]
fn test_address_str_preserves_https() {
    assert_eq!(
        address_str("https://127.0.0.1:9000"),
        "https://127.0.0.1:9000"
    );
    assert_eq!(address_str("https://node1:9000"), "https://node1:9000");
    assert_eq!(
        address_str("https://secure.example.com:443"),
        "https://secure.example.com:443"
    );
}

#[test]
fn test_address_str_no_duplication() {
    // Ensure we don't accidentally create double schemes
    assert_eq!(
        address_str("http://127.0.0.1:9000"),
        "http://127.0.0.1:9000"
    );
    assert_eq!(
        address_str("https://127.0.0.1:9000"),
        "https://127.0.0.1:9000"
    );
}
