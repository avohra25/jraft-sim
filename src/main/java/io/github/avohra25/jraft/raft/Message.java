package io.github.avohra25.jraft.raft;

import java.util.List;

import io.github.avohra25.jraft.kv.Command;

/** Everything that travels over the simulated network: Raft RPCs plus client traffic. */
public sealed interface Message {

    /** Raft RPCs carry the sender's term; receivers step down when they see a newer one. */
    sealed interface RaftRpc extends Message {
        long term();
    }

    /**
     * With {@code preVote} set this only asks "would you vote for me in {@code term}?": the
     * receiver changes no state, and {@code term} is the term the sender would campaign in, not
     * one it has adopted.
     */
    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements RaftRpc {
        public RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
            this(term, candidateId, lastLogIndex, lastLogTerm, false);
        }
    }

    /** A granted pre-vote echoes the proposed term; every other reply carries the voter's term. */
    record RequestVoteReply(long term, boolean granted, boolean preVote) implements RaftRpc {
        public RequestVoteReply(long term, boolean granted) {
            this(term, granted, false);
        }
    }

    record AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements RaftRpc {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * On success {@code matchIndex} is the last index known to match the leader. On failure,
     * {@code conflictTerm}/{@code conflictIndex} let the leader skip a whole term per round trip
     * instead of decrementing nextIndex one entry at a time ({@code conflictTerm == -1} means the
     * follower's log is simply too short).
     */
    record AppendEntriesReply(long term, boolean success, long matchIndex,
                              long conflictIndex, long conflictTerm) implements RaftRpc {}

    record InstallSnapshot(long term, int leaderId, long lastIncludedIndex, long lastIncludedTerm,
                           byte[] data) implements RaftRpc {}

    record InstallSnapshotReply(long term, long matchIndex) implements RaftRpc {}

    record ClientRequest(Command.ClientCommand command) implements Message {}

    /** {@code leaderHint} is -1 when the contacted node does not know who the leader is. */
    record ClientReply(int clientId, long seq, boolean ok, String result, int leaderHint) implements Message {}
}
