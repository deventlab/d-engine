use super::lease::LeaseConfig;

#[test]
fn test_default_config() {
    let config = LeaseConfig::default();
    assert_eq!(config.cleanup_interval_ms, 1000);
    assert_eq!(config.max_cleanup_duration_ms, 1);
    assert!(config.validate().is_ok());
}

#[test]
fn test_explicit_config() {
    let config = LeaseConfig {
        cleanup_interval_ms: 5000,
        max_cleanup_duration_ms: 5,
    };
    assert_eq!(config.cleanup_interval_ms, 5000);
    assert_eq!(config.max_cleanup_duration_ms, 5);
    assert!(config.validate().is_ok());
}

#[test]
fn test_validation_cleanup_interval_ms() {
    let base = LeaseConfig::default();

    // Too small
    assert!(
        LeaseConfig {
            cleanup_interval_ms: 99,
            ..base
        }
        .validate()
        .is_err()
    );
    // Too large
    assert!(
        LeaseConfig {
            cleanup_interval_ms: 60_001,
            ..base
        }
        .validate()
        .is_err()
    );
    // Valid range
    assert!(
        LeaseConfig {
            cleanup_interval_ms: 100,
            ..base
        }
        .validate()
        .is_ok()
    );
    assert!(
        LeaseConfig {
            cleanup_interval_ms: 1000,
            ..base
        }
        .validate()
        .is_ok()
    );
    assert!(
        LeaseConfig {
            cleanup_interval_ms: 60_000,
            ..base
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn test_validation_max_cleanup_duration() {
    let base = LeaseConfig::default();

    // Too small
    assert!(
        LeaseConfig {
            max_cleanup_duration_ms: 0,
            ..base
        }
        .validate()
        .is_err()
    );
    // Too large
    assert!(
        LeaseConfig {
            max_cleanup_duration_ms: 101,
            ..base
        }
        .validate()
        .is_err()
    );
    // Valid range
    assert!(
        LeaseConfig {
            max_cleanup_duration_ms: 1,
            ..base
        }
        .validate()
        .is_ok()
    );
    assert!(
        LeaseConfig {
            max_cleanup_duration_ms: 50,
            ..base
        }
        .validate()
        .is_ok()
    );
    assert!(
        LeaseConfig {
            max_cleanup_duration_ms: 100,
            ..base
        }
        .validate()
        .is_ok()
    );
}

#[test]
fn test_typical_production_config() {
    let config = LeaseConfig {
        cleanup_interval_ms: 1000,
        max_cleanup_duration_ms: 1,
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_aggressive_cleanup_config() {
    let config = LeaseConfig {
        cleanup_interval_ms: 100,
        max_cleanup_duration_ms: 10,
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_relaxed_cleanup_config() {
    let config = LeaseConfig {
        cleanup_interval_ms: 60_000,
        max_cleanup_duration_ms: 5,
    };
    assert!(config.validate().is_ok());
}
