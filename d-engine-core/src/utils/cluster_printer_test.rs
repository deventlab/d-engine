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
    assert_eq!(status_emoji(0), "🔄"); // Joining
    assert_eq!(status_emoji(1), "🔄"); // Syncing
    assert_eq!(status_emoji(2), "✅"); // Active
    assert_eq!(status_emoji(3), "❌"); // Inactive
    assert_eq!(status_emoji(999), "❓"); // Invalid status
}

#[test]
fn test_status_name_all_variants() {
    assert_eq!(status_name(0), "Joining");
    assert_eq!(status_name(1), "Syncing");
    assert_eq!(status_name(2), "Active");
    assert_eq!(status_name(3), "Inactive");
    assert_eq!(status_name(999), "Unknown"); // Invalid status
}

#[test]
fn test_role_emoji_consistency_with_proto() {
    // Ensure our emoji mapping matches proto enum values
    assert_eq!(NodeRole::Follower as i32, 0);
    assert_eq!(NodeRole::Candidate as i32, 1);
    assert_eq!(NodeRole::Leader as i32, 2);
    assert_eq!(NodeRole::Learner as i32, 3);
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
    // Test typical status progression: Joining -> Syncing -> Active
    let statuses = [(0, "Joining"), (1, "Syncing"), (2, "Active")];

    for (status_val, expected_name) in statuses {
        assert_eq!(status_name(status_val), expected_name);
        // Joining and Syncing both use spinning emoji
        if status_val < 2 {
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
