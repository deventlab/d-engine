use super::lease::LeaseConfig;

#[test]
fn test_default_config() {
    let config = LeaseConfig::default();
    assert!(!config.enabled); // Default: disabled
    assert_eq!(config.interval_ms, 1000);
    assert_eq!(config.max_cleanup_duration_ms, 1);
    assert!(config.validate().is_ok());
}

#[test]
fn test_enabled_config() {
    let config = LeaseConfig {
        enabled: true,
        interval_ms: 5000,
        max_cleanup_duration_ms: 5,
    };
    assert!(config.enabled);
    assert_eq!(config.interval_ms, 5000);
    assert_eq!(config.max_cleanup_duration_ms, 5);
    assert!(config.validate().is_ok());
}

#[test]
fn test_disabled_config_skips_validation() {
    let config = LeaseConfig {
        enabled: false,
        interval_ms: 50,            // Invalid (too small), but ignored when disabled
        max_cleanup_duration_ms: 0, // Invalid, but ignored when disabled
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_validation_interval_ms() {
    let mut config = LeaseConfig {
        enabled: true,
        ..Default::default()
    };

    // Too small
    config.interval_ms = 99;
    assert!(config.validate().is_err());

    // Too large
    config.interval_ms = 3_600_001;
    assert!(config.validate().is_err());

    // Valid range
    config.interval_ms = 100;
    assert!(config.validate().is_ok());

    config.interval_ms = 1000;
    assert!(config.validate().is_ok());

    config.interval_ms = 3_600_000;
    assert!(config.validate().is_ok());
}

#[test]
fn test_validation_max_cleanup_duration() {
    let mut config = LeaseConfig {
        enabled: true,
        ..Default::default()
    };

    // Too small
    config.max_cleanup_duration_ms = 0;
    assert!(config.validate().is_err());

    // Too large
    config.max_cleanup_duration_ms = 101;
    assert!(config.validate().is_err());

    // Valid range
    config.max_cleanup_duration_ms = 1;
    assert!(config.validate().is_ok());

    config.max_cleanup_duration_ms = 50;
    assert!(config.validate().is_ok());

    config.max_cleanup_duration_ms = 100;
    assert!(config.validate().is_ok());
}

#[test]
fn test_typical_production_config() {
    let config = LeaseConfig {
        enabled: true,
        interval_ms: 1000,
        max_cleanup_duration_ms: 1,
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_aggressive_cleanup_config() {
    let config = LeaseConfig {
        enabled: true,
        interval_ms: 100,
        max_cleanup_duration_ms: 10,
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_relaxed_cleanup_config() {
    let config = LeaseConfig {
        enabled: true,
        interval_ms: 10_000,
        max_cleanup_duration_ms: 5,
    };
    assert!(config.validate().is_ok());
}
