package io.github.avohra25.jraft.raft;

/**
 * Timing and batching knobs, all in simulated milliseconds.
 *
 * @param electionTimeoutMin lower bound of the randomized election timeout
 * @param electionTimeoutMax upper bound of the randomized election timeout
 * @param heartbeatInterval  how often a leader sends AppendEntries when idle
 * @param maxEntriesPerAppend batch limit for a single AppendEntries
 * @param snapshotThreshold  compact once this many applied entries sit in the log (0 = never)
 */
public record RaftConfig(long electionTimeoutMin, long electionTimeoutMax, long heartbeatInterval,
                         int maxEntriesPerAppend, int snapshotThreshold) {

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

    public static RaftConfig defaults() {
        return new RaftConfig(150, 300, 50, 64, 0);
    }

    public RaftConfig withSnapshotThreshold(int threshold) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, threshold);
    }
}
