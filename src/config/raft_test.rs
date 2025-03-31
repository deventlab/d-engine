use log::error;

use crate::{ElectionConfig, RaftConfig};

#[test]
fn test_invalid_election_timeout() {
    let mut config = RaftConfig::default();
    config.election.election_timeout_min = 1000;
    config.election.election_timeout_max = 500;

    assert!(config.validate().is_err());
}

#[test]
fn test_valid_config() {
    let config = RaftConfig {
        election: ElectionConfig {
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        },
        ..Default::default()
    };

    if let Err(_) = config.validate() {
        assert!(false);
    } else {
        assert!(true);
    }
}
