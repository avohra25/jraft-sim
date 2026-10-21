package io.github.avohra25.jraft.raft;

/**
 * Everything a Raft node keeps on stable storage: current term, vote, log and latest snapshot.
 *
 * <p>The simulator models a disk that survives crashes: the cluster owns one of these per node
 * and hands the same instance back on restart, while all volatile state is rebuilt from scratch.
 * Mutations are treated as synchronously durable, so a node never replies to an RPC before the
 * state that reply depends on is "on disk".
 */
public final class PersistentState {

    public static final int NO_VOTE = -1;

    private final RaftLog log = new RaftLog();
    private long currentTerm;
    private int votedFor = NO_VOTE;
    private byte[] snapshot;
    private long writes;

    public long currentTerm() {
        return currentTerm;
    }

    public int votedFor() {
        return votedFor;
    }

    public RaftLog log() {
        return log;
    }

    /** Moves to a new term, clearing the vote; terms never go backwards. */
    public void advanceTerm(long term) {
        if (term < currentTerm) {
            throw new IllegalArgumentException("term went backwards: " + term + " < " + currentTerm);
        }
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = NO_VOTE;
            writes++;
        }
    }

    public void voteFor(int candidate) {
        if (votedFor != NO_VOTE && votedFor != candidate) {
            throw new IllegalStateException("already voted for " + votedFor + " in term " + currentTerm);
        }
        votedFor = candidate;
        writes++;
    }

    public byte[] snapshot() {
        return snapshot;
    }

    public void saveSnapshot(byte[] snapshot) {
        this.snapshot = snapshot.clone();
        writes++;
    }

    /** Number of term/vote writes, a rough proxy for fsync count. */
    public long writes() {
        return writes;
    }
}
