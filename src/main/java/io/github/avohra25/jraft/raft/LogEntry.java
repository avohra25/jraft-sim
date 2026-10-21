package io.github.avohra25.jraft.raft;

import io.github.avohra25.jraft.kv.Command;

/** One entry of the replicated log. Indexes start at 1; index 0 is the empty-log sentinel. */
public record LogEntry(long term, long index, Command command) {
    public LogEntry {
        if (term < 1 || index < 1) {
            throw new IllegalArgumentException("term and index start at 1");
        }
    }
}
