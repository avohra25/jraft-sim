package io.github.avohra25.jraft.raft;

import java.util.List;

import io.github.avohra25.jraft.kv.Command;

/** Everything that travels over the simulated network: Raft RPCs plus client traffic. */
public sealed interface Message {

    /** Raft RPCs carry the sender's term; receivers step down when they see a newer one. */
    sealed interface RaftRpc extends Message {
        long term();
    }

    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) implements RaftRpc {}

    record RequestVoteReply(long term, boolean granted) implements RaftRpc {}

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

    record ClientRequest(Command.ClientCommand command) implements Message {}

    /** {@code leaderHint} is -1 when the contacted node does not know who the leader is. */
    record ClientReply(int clientId, long seq, boolean ok, String result, int leaderHint) implements Message {}
}
