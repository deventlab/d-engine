use crate::PeerUpdate;

fn update(
    match_index: Option<u64>,
    next_index: u64,
) -> PeerUpdate {
    PeerUpdate {
        match_index,
        next_index,
        success: true,
    }
}

#[test]
fn test_match_index_past_the_leader_log_is_capped() {
    let bounded = update(Some(50), 51).bounded_by_leader_log(20);

    assert_eq!(bounded.match_index, Some(20));
}

#[test]
fn test_next_index_past_the_entry_after_the_leader_log_is_capped() {
    let bounded = update(Some(50), 51).bounded_by_leader_log(20);

    assert_eq!(bounded.next_index, 21);
}

#[test]
fn test_update_inside_the_leader_log_is_unchanged() {
    let original = update(Some(15), 16);

    assert_eq!(original.clone().bounded_by_leader_log(20), original);
}

#[test]
fn test_update_exactly_at_the_leader_log_end_is_unchanged() {
    let original = update(Some(20), 21);

    assert_eq!(original.clone().bounded_by_leader_log(20), original);
}

#[test]
fn test_missing_match_index_stays_missing() {
    let bounded = update(None, 1).bounded_by_leader_log(20);

    assert_eq!(bounded.match_index, None);
}

#[test]
fn test_conflict_hint_below_the_leader_log_is_not_raised() {
    let conflict = PeerUpdate {
        match_index: None,
        next_index: 4,
        success: false,
    };

    assert_eq!(conflict.clone().bounded_by_leader_log(20), conflict);
}

#[test]
fn test_empty_leader_log_caps_everything_to_the_start() {
    let bounded = update(Some(5), 6).bounded_by_leader_log(0);

    assert_eq!(bounded.match_index, Some(0));
    assert_eq!(bounded.next_index, 1);
}

#[test]
fn test_success_flag_is_preserved() {
    let rejected = PeerUpdate {
        match_index: Some(50),
        next_index: 51,
        success: false,
    };

    assert!(!rejected.bounded_by_leader_log(20).success);
}
