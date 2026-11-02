package io.github.avohra25.jraft.raft;

/** Observation hooks used by the harness, the timeline and the invariant checker. */
public interface RaftListener {

    default void onRoleChange(RaftNode node, RaftNode.Role previous) {}

    /** A node's election timer fired and it is asking for pre-votes for {@code term}. */
    default void onPreVote(RaftNode node, long term) {}

    /** Progress of a leadership transfer on the leader: {@code what} is a short description. */
    default void onTransfer(RaftNode leader, int target, String what) {}

    /** A leader that has not heard from a majority within one election timeout is giving up. */
    default void onLostQuorum(RaftNode leader, int reachable) {}

    /** Called on the leader each time it advances its commit index by counting replicas. */
    default void onLeaderCommit(RaftNode leader, long commitIndex) {}

    default void onApply(RaftNode node, LogEntry entry, String result) {}

    /** {@code installed} is true when the snapshot came from the leader rather than local compaction. */
    default void onSnapshot(RaftNode node, long lastIncludedIndex, boolean installed) {}
}
