package io.github.avohra25.jraft.raft;

/**
 * Timing and batching knobs, all in simulated milliseconds.
 *
 * @param electionTimeoutMin lower bound of the randomized election timeout
 * @param electionTimeoutMax upper bound of the randomized election timeout
 * @param heartbeatInterval  how often a leader sends AppendEntries when idle
 * @param maxEntriesPerAppend batch limit for a single AppendEntries
 * @param snapshotThreshold  compact once this many applied entries sit in the log (0 = never)
 * @param unsafeCommitRule   <b>deliberately broken</b>: let leaders commit entries from earlier
 *                           terms by counting replicas (the bug in Figure 8 of the Raft paper).
 *                           Only exists so tests can show the checkers catch it.
 */
public record RaftConfig(long electionTimeoutMin, long electionTimeoutMax, long heartbeatInterval,
                         int maxEntriesPerAppend, int snapshotThreshold, boolean unsafeCommitRule) {

    public RaftConfig {
        if (electionTimeoutMin <= 0 || electionTimeoutMax < electionTimeoutMin) {
            throw new IllegalArgumentException("bad election timeout range");
        }
        if (heartbeatInterval <= 0 || heartbeatInterval >= electionTimeoutMin) {
            throw new IllegalArgumentException("heartbeat must be shorter than the election timeout");
        }
        if (maxEntriesPerAppend < 1) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
        }
        if (snapshotThreshold < 0) {
            throw new IllegalArgumentException("snapshotThreshold must be >= 0");
        }
    }

    public RaftConfig(long electionTimeoutMin, long electionTimeoutMax, long heartbeatInterval,
                      int maxEntriesPerAppend, int snapshotThreshold) {
        this(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, snapshotThreshold, false);
    }

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 64, 0);
    }

    public RaftConfig withSnapshotThreshold(int threshold) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, threshold,
                unsafeCommitRule);
    }

    public RaftConfig withMaxEntriesPerAppend(int max) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, max, snapshotThreshold,
                unsafeCommitRule);
    }

    public RaftConfig withElectionTimeout(long min, long max) {
        return new RaftConfig(min, max, heartbeatInterval, maxEntriesPerAppend, snapshotThreshold, unsafeCommitRule);
    }

    /** See {@link #unsafeCommitRule()}: never enable this outside of checker tests. */
    public RaftConfig withUnsafeCommitRule(boolean unsafe) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend,
                snapshotThreshold, unsafe);
    }
}
