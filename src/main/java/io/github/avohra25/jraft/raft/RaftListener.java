package io.github.avohra25.jraft.raft;

/** Observation hooks used by the harness, the timeline and the invariant checker. */
public interface RaftListener {

    default void onRoleChange(RaftNode node, RaftNode.Role previous) {}

    /** Called on the leader each time it advances its commit index by counting replicas. */
    default void onLeaderCommit(RaftNode leader, long commitIndex) {}

    default void onApply(RaftNode node, LogEntry entry, String result) {}
}
