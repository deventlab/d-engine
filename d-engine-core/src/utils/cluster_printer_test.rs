use d_engine_proto::common::NodeRole;

use super::cluster_printer::*;

#[test]
fn test_role_emoji_all_variants() {
    assert_eq!(role_emoji(NodeRole::Follower as i32), "📮");
    assert_eq!(role_emoji(NodeRole::Candidate as i32), "🗳️");
    assert_eq!(role_emoji(NodeRole::Leader as i32), "👑");
    assert_eq!(role_emoji(NodeRole::Learner as i32), "🎓");
    assert_eq!(role_emoji(999), "❓"); // Invalid role
}

#[test]
fn test_role_name_all_variants() {
    assert_eq!(role_name(NodeRole::Follower as i32), "Follower");
    assert_eq!(role_name(NodeRole::Candidate as i32), "Candidate");
    assert_eq!(role_name(NodeRole::Leader as i32), "Leader");
    assert_eq!(role_name(NodeRole::Learner as i32), "Learner");
    assert_eq!(role_name(999), "Unknown"); // Invalid role
}

#[test]
fn test_status_emoji_all_variants() {
    use d_engine_proto::common::NodeStatus;
    assert_eq!(status_emoji(NodeStatus::Unspecified as i32), "❓");
    assert_eq!(status_emoji(NodeStatus::Promotable as i32), "🔄");
    assert_eq!(status_emoji(NodeStatus::ReadOnly as i32), "🔄");
    assert_eq!(status_emoji(NodeStatus::Active as i32), "✅");
    assert_eq!(status_emoji(999), "❓"); // Invalid status
}

#[test]
fn test_status_name_all_variants() {
    use d_engine_proto::common::NodeStatus;
    assert_eq!(status_name(NodeStatus::Unspecified as i32), "Unspecified");
    assert_eq!(status_name(NodeStatus::Promotable as i32), "Promotable");
    assert_eq!(status_name(NodeStatus::ReadOnly as i32), "ReadOnly");
    assert_eq!(status_name(NodeStatus::Active as i32), "Active");
    assert_eq!(status_name(999), "Unknown"); // Invalid status
}

#[test]
fn test_role_emoji_consistency_with_proto() {
    // Ensure our emoji mapping matches proto enum values (after adding UNSPECIFIED=0)
    assert_eq!(NodeRole::Follower as i32, 1);
    assert_eq!(NodeRole::Candidate as i32, 2);
    assert_eq!(NodeRole::Leader as i32, 3);
    assert_eq!(NodeRole::Learner as i32, 4);
}

#[test]
fn test_role_and_emoji_pairing() {
    let roles = [
        (NodeRole::Leader as i32, "👑", "Leader"),
        (NodeRole::Follower as i32, "📮", "Follower"),
        (NodeRole::Learner as i32, "🎓", "Learner"),
    ];

    for (role_val, expected_emoji, expected_name) in roles {
        assert_eq!(role_emoji(role_val), expected_emoji);
        assert_eq!(role_name(role_val), expected_name);
    }
}

#[test]
fn test_status_progression() {
    use d_engine_proto::common::NodeStatus;
    // Test NodeStatus enum values: Unspecified -> Promotable -> ReadOnly -> Active
    let statuses = [
        (NodeStatus::Unspecified as i32, "Unspecified"),
        (NodeStatus::Promotable as i32, "Promotable"),
        (NodeStatus::ReadOnly as i32, "ReadOnly"),
        (NodeStatus::Active as i32, "Active"),
    ];

    for (status_val, expected_name) in statuses {
        assert_eq!(status_name(status_val), expected_name);
        // Promotable and ReadOnly both use spinning emoji (learner states)
        if status_val == NodeStatus::Promotable as i32 || status_val == NodeStatus::ReadOnly as i32
        {
            assert_eq!(status_emoji(status_val), "🔄");
        }
    }
}

#[test]
fn test_edge_cases() {
    // Test negative values (shouldn't happen but should handle gracefully)
    assert_eq!(role_emoji(-1), "❓");
    assert_eq!(role_name(-1), "Unknown");
    assert_eq!(status_emoji(-1), "❓");
    assert_eq!(status_name(-1), "Unknown");
}

#[test]
fn test_output_formatting_consistency() {
    // Ensure emoji strings are single characters (for alignment)
    let emojis = ["❓", "📮", "👑", "🎓", "🔄", "✅", "❌"];
    for emoji in emojis {
        // Each emoji should be non-empty
        assert!(!emoji.is_empty());
        // Emojis are multi-byte but should be single grapheme
        assert_eq!(emoji.chars().count(), 1);
    }
}
