// Cluster Status Printer Module
//
// This module provides utilities for printing clear, user-friendly cluster status
// information to help administrators understand the cluster state at a glance.
//
// Implements:
// - Plan A: Enhanced startup output with cluster membership table
// - Plan B: Clear role transition messages
//
// Context for Plan D (CLI tool):
// - These formatting functions can be reused by the future CLI tool
// - The data structures used here match what the CLI will need to query
// - Emoji and color codes can be made configurable for different terminals

use d_engine_proto::common::NodeRole;

use crate::Membership;
use crate::TypeConfig;

/// Get emoji representation for node role
///
/// Maps NodeRole enum values to emoji:
/// - Follower (1): 📮
/// - Candidate (2): 🗳️
/// - Leader (3): 👑
/// - Learner (4): 🎓
pub fn role_emoji(role: i32) -> &'static str {
    use d_engine_proto::common::NodeRole;
    match role {
        r if r == NodeRole::Follower as i32 => "📮",
        r if r == NodeRole::Candidate as i32 => "🗳️",
        r if r == NodeRole::Leader as i32 => "👑",
        r if r == NodeRole::Learner as i32 => "🎓",
        _ => "❓", // Unknown/Invalid
    }
}

/// Get human-readable role name
///
/// Maps NodeRole enum values to display names:
/// - Follower (1)
/// - Candidate (2)
/// - Leader (3)
/// - Learner (4)
pub fn role_name(role: i32) -> &'static str {
    use d_engine_proto::common::NodeRole;
    match role {
        r if r == NodeRole::Follower as i32 => "Follower",
        r if r == NodeRole::Candidate as i32 => "Candidate",
        r if r == NodeRole::Leader as i32 => "Leader",
        r if r == NodeRole::Learner as i32 => "Learner",
        _ => "Unknown",
    }
}

/// Get emoji representation for node status
pub fn status_emoji(status: i32) -> &'static str {
    use d_engine_proto::common::NodeStatus;
    match status {
        s if s == NodeStatus::Unspecified as i32 => "❓", // Unspecified
        s if s == NodeStatus::Promotable as i32 => "🔄",  // Promotable (Learner)
        s if s == NodeStatus::ReadOnly as i32 => "🔄",    // ReadOnly (Learner)
        s if s == NodeStatus::Active as i32 => "✅",      // Active (Voter)
        _ => "❓",
    }
}

/// Get human-readable status name
pub fn status_name(status: i32) -> &'static str {
    use d_engine_proto::common::NodeStatus;
    match status {
        s if s == NodeStatus::Unspecified as i32 => "Unspecified",
        s if s == NodeStatus::Promotable as i32 => "Promotable",
        s if s == NodeStatus::ReadOnly as i32 => "ReadOnly",
        s if s == NodeStatus::Active as i32 => "Active",
        _ => "Unknown",
    }
}

/// Print the cluster membership table
///
/// Example output:
/// ```text
/// ================================================================================
///   CLUSTER MEMBERSHIP:
///   Node ID  Address              Role         Status
///   ------------------------------------------------------------
///   1        0.0.0.0:9081        👑 Leader     ✅ Active
///   2        0.0.0.0:9082        🎓 Learner    🔄 Syncing
/// ================================================================================
/// ```
pub async fn print_cluster_membership_table<T: TypeConfig, M: Membership<T>>(membership: &M) {
    let members = membership.get_all_nodes().await;

    println!("\n{}", "=".repeat(80));
    println!("  CLUSTER MEMBERSHIP:");
    println!(
        "  {:<8} {:<20} {:<18} {:<15}",
        "Node ID", "Address", "Role", "Status"
    );
    println!("  {}", "-".repeat(80));

    for member in members {
        println!(
            "  {:<8} {:<20} {:<18} {:<15}",
            member.id,
            member.address,
            format!("{} {}", role_emoji(member.role), role_name(member.role)),
            format!(
                "{} {}",
                status_emoji(member.status),
                status_name(member.status)
            )
        );
    }

    println!("{}", "=".repeat(80));
}

/// Print node startup banner with full status
///
/// This is the main entry point for Plan A: Enhanced startup output
///
/// Example output:
/// ```text
/// ================================================================================
///   D-ENGINE NODE STARTED
/// ================================================================================
///   Node ID:        2
///   Listen Address: 0.0.0.0:9082
///   Current Role:   🎓 Learner
///   Current Term:   2
///   Leader ID:      1
/// ================================================================================
///   CLUSTER MEMBERSHIP:
///   ...membership table...
/// ================================================================================
/// ```
pub async fn print_node_startup_banner<T: TypeConfig, M: Membership<T>>(
    node_id: u32,
    listen_addr: &str,
    current_role: i32,
    current_term: u64,
    leader_id: Option<u32>,
    membership: &M,
) {
    println!("\n{}", "=".repeat(80));
    println!("  D-ENGINE NODE STARTED");
    println!("{}", "=".repeat(80));
    println!("  Node ID:        {node_id}");
    println!("  Listen Address: {listen_addr}");
    println!(
        "  Current Role:   {} {}",
        role_emoji(current_role),
        role_name(current_role)
    );
    println!("  Current Term:   {current_term}");
    println!(
        "  Leader ID:      {}",
        leader_id.map(|id| id.to_string()).unwrap_or_else(|| "None".to_string())
    );
    println!("{}", "=".repeat(80));

    print_cluster_membership_table(membership).await;
}

/// Print learner join success message
///
/// Part of Plan B: Clear role transition messages
///
/// Example output:
/// ```text
/// 🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉
///   ✅ NODE 2 SUCCESSFULLY JOINED CLUSTER
///   Role: 🎓 Learner → Syncing data from Leader 1
///   Next: Will auto-promote to Voter when caught up (if Promotable status)"
/// 🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉
/// ```
pub fn print_learner_join_success(
    node_id: u32,
    leader_id: u32,
) {
    println!("\n{}", "🎉".repeat(40));
    println!("  ✅ NODE {node_id} SUCCESSFULLY JOINED CLUSTER");
    println!(
        "  Role: {} Learner → Syncing data from Leader {leader_id}",
        role_emoji(NodeRole::Learner as i32)
    );
    println!("  Next: Will auto-promote to Voter when caught up (if Promotable status)");
    println!("{}\n", "🎉".repeat(40));
}

/// Print learner promotion success message
///
/// Part of Plan B: Clear role transition messages
///
/// Example output:
/// ```text
/// 🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀
///   🎊 NODE 2 PROMOTED TO VOTER!
///   Old Role: 🎓 Learner
///   New Role: 📮 Follower (Voter)
///   Status: Can now participate in voting and consensus
/// 🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀
/// ```
pub fn print_learner_promoted_to_voter(node_id: u32) {
    println!("\n{}", "🚀".repeat(40));
    println!("  🎊 NODE {node_id} PROMOTED TO VOTER!");
    println!(
        "  Old Role: {} Learner",
        role_emoji(NodeRole::Learner as i32)
    );
    println!(
        "  New Role: {} Follower (Voter)",
        role_emoji(NodeRole::Follower as i32)
    );
    println!("  Status: Can now participate in voting and consensus");
    println!("{}\n", "🚀".repeat(40));
}

/// Print role transition message
///
/// Part of Plan B: Clear role transition messages
///
/// This is called whenever a node changes roles (Follower→Candidate, Candidate→Leader, etc.)
///
/// Example output:
/// ```text
/// ================================================================================
///   🔄 ROLE TRANSITION
/// ================================================================================
///   Node ID:     1
///   From Role:   📮 Follower
///   To Role:     👑 Leader
///   Term:        2
///   Reason:      Won election with majority votes
/// ================================================================================
/// ```
pub fn print_role_transition(
    node_id: u32,
    from_role: i32,
    to_role: i32,
    term: u64,
    reason: &str,
) {
    println!("\n{}", "=".repeat(80));
    println!("  🔄 ROLE TRANSITION");
    println!("{}", "=".repeat(80));
    println!("  Node ID:     {node_id}");
    println!(
        "  From Role:   {} {}",
        role_emoji(from_role),
        role_name(from_role)
    );
    println!(
        "  To Role:     {} {}",
        role_emoji(to_role),
        role_name(to_role)
    );
    println!("  Term:        {term}");
    println!("  Reason:      {reason}");
    println!("{}", "=".repeat(80));
}

/// Print leader accepting new node message
///
/// Part of Plan B: Clear role transition messages
///
/// Called on the leader when a new node joins
///
/// Example output:
/// ```text
/// ================================================================================
///   ✅ LEADER: ACCEPTING NEW NODE
/// ================================================================================
///   Leader ID:       1
///   New Node ID:     2
///   New Node Addr:   0.0.0.0:9082
///   Role:            🎓 Learner
///   Action:          AddNode config change committed
/// ================================================================================
/// ```
pub fn print_leader_accepting_new_node(
    leader_id: u32,
    new_node_id: u32,
    new_node_addr: &str,
    role: i32,
) {
    println!("\n{}", "=".repeat(80));
    println!("  ✅ LEADER: ACCEPTING NEW NODE");
    println!("{}", "=".repeat(80));
    println!("  Leader ID:       {leader_id}");
    println!("  New Node ID:     {new_node_id}");
    println!("  New Node Addr:   {new_node_addr}");
    println!(
        "  Role:            {} {}",
        role_emoji(role),
        role_name(role)
    );
    println!("  Action:          AddNode config change committed");
    println!("{}", "=".repeat(80));
}

/// Print leader triggering learner promotion message
///
/// Part of Plan B: Clear role transition messages
///
/// Called on the leader when it decides to promote a learner
///
/// Example output:
/// ```text
/// ================================================================================
///   🚀 LEADER: PROMOTING LEARNER
/// ================================================================================
///   Leader ID:           1
///   Learner ID:          2
///   Learner Match Index: 150
///   Leader Commit Index: 150
///   Progress:            100.0%
///   Action:              Scheduling PromoteToVoter config change
/// ================================================================================
/// ```
pub fn print_leader_promoting_learner(
    leader_id: u32,
    learner_id: u32,
    match_index: u64,
    commit_index: u64,
) {
    let progress = if commit_index > 0 {
        (match_index as f64 / commit_index as f64) * 100.0
    } else {
        100.0
    };

    println!("\n{}", "=".repeat(80));
    println!("  🚀 LEADER: PROMOTING LEARNER");
    println!("{}", "=".repeat(80));
    println!("  Leader ID:           {leader_id}");
    println!("  Learner ID:          {learner_id}");
    println!("  Learner Match Index: {match_index}");
    println!("  Leader Commit Index: {commit_index}");
    println!("  Progress:            {progress:.1}%");
    println!("  Action:              Scheduling PromoteToVoter config change");
    println!("{}", "=".repeat(80));
}
