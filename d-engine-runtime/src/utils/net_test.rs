use crate::net::address_str;

#[test]
fn test_address_str() {
    // Test with plain address
    assert_eq!(address_str("127.0.0.1:8080"), "http://127.0.0.1:8080");

    // Test with http:// prefix
    assert_eq!(
        address_str("http://127.0.0.1:8080"),
        "http://127.0.0.1:8080"
    );

    // Test with https:// prefix
    assert_eq!(
        address_str("https://127.0.0.1:8080"),
        "http://127.0.0.1:8080"
    );

    // Test with hostname
    assert_eq!(address_str("node1:8080"), "http://node1:8080");
}
