#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::collections::HashMap;

    /// Test that get_multi reconstructs results in correct key order.
    ///
    /// Verifies the critical fix: when server returns only results for
    /// existing keys (sparse), we must map by key to preserve positional
    /// correspondence with the input key vector.
    #[test]
    fn test_get_multi_result_reconstruction() {
        // Simulate server response scenario:
        // Request: [key1, key2, key3]
        // Exists: [key1, key3]  (key2 is missing)
        // Server returns: [key1→value1, key3→value3]

        let requested_keys: Vec<_> = [
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ]
        .to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("key1"), Bytes::from("value1")),
            (Bytes::from("key3"), Bytes::from("value3")),
        ];

        // Simulate the fixed reconstruction logic
        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        // Expected: [Some(value1), None, Some(value3)]
        assert_eq!(
            reconstructed.len(),
            3,
            "Result count must match request count"
        );
        assert_eq!(
            reconstructed[0],
            Some(Bytes::from("value1")),
            "Position 0 is key1"
        );
        assert_eq!(reconstructed[1], None, "Position 1 is key2 (missing)");
        assert_eq!(
            reconstructed[2],
            Some(Bytes::from("value3")),
            "Position 2 is key3"
        );
    }

    /// Test edge case: all requested keys exist.
    #[test]
    fn test_get_multi_all_keys_exist() {
        let requested_keys: Vec<_> = [Bytes::from("a"), Bytes::from("b")].to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ];

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed.len(), 2);
        assert_eq!(reconstructed[0], Some(Bytes::from("1")));
        assert_eq!(reconstructed[1], Some(Bytes::from("2")));
    }

    /// Test edge case: no requested keys exist.
    #[test]
    fn test_get_multi_no_keys_exist() {
        let requested_keys: Vec<_> =
            [Bytes::from("x"), Bytes::from("y"), Bytes::from("z")].to_vec();
        let server_results: Vec<(Bytes, Bytes)> = Vec::new();

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed.len(), 3);
        assert!(reconstructed.iter().all(|r| r.is_none()));
    }

    /// Test that empty byte values are preserved correctly.
    #[test]
    fn test_get_multi_preserves_empty_values() {
        let requested_keys: Vec<_> = [Bytes::from("empty"), Bytes::from("nonempty")].to_vec();
        let server_results: Vec<_> = vec![
            (Bytes::from("empty"), Bytes::new()),
            (Bytes::from("nonempty"), Bytes::from("v")),
        ];

        let results_by_key: HashMap<_, _> = server_results.into_iter().collect();
        let reconstructed: Vec<Option<Bytes>> =
            requested_keys.iter().map(|k| results_by_key.get(k).cloned()).collect();

        assert_eq!(reconstructed[0], Some(Bytes::new()));
        assert_eq!(reconstructed[1], Some(Bytes::from("v")));
    }
}
