package io.github.avohra25.jraft.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The replicated log, addressed by absolute 1-based index.
 *
 * <p>The log only stores entries after {@link #snapshotIndex()}; everything at or before that
 * index has been folded into a snapshot. With no snapshot the base is the sentinel index 0 with
 * term 0, so {@code termAt(0) == 0} and the log-matching check for the very first entry works
 * like any other.
 */
public final class RaftLog {

    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private long snapshotIndex;
    private long snapshotTerm;

    public long snapshotIndex() {
        return snapshotIndex;
    }

    public long snapshotTerm() {
        return snapshotTerm;
    }

    public long firstIndex() {
        return snapshotIndex + 1;
    }

    public long lastIndex() {
        return snapshotIndex + entries.size();
    }

    public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.getLast().term();
    }

    /** Number of entries held in memory (i.e. not yet compacted). */
    public int size() {
        return entries.size();
    }

    /** True if {@code termAt(index)} is answerable: the snapshot base or a retained entry. */
    public boolean hasTermAt(long index) {
        return index >= snapshotIndex && index <= lastIndex();
    }

    public long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        return entry(index).term();
    }

    public LogEntry entry(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IndexOutOfBoundsException(
                    "index " + index + " outside retained range [" + firstIndex() + ", " + lastIndex() + "]");
        }
        return entries.get((int) (index - snapshotIndex - 1));
    }

    /** Up to {@code max} entries starting at {@code from}. */
    public List<LogEntry> slice(long from, int max) {
        if (from <= snapshotIndex) {
            throw new IndexOutOfBoundsException("index " + from + " is compacted");
        }
        int start = (int) (from - snapshotIndex - 1);
        int end = Math.min(entries.size(), start + max);
        return start >= end ? List.of() : List.copyOf(entries.subList(start, end));
    }

    public List<LogEntry> entries() {
        return Collections.unmodifiableList(entries);
    }

    public void append(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalArgumentException("expected index " + (lastIndex() + 1) + " but got " + entry.index());
        }
        if (entry.term() < lastTerm()) {
            throw new IllegalArgumentException("term went backwards: " + entry.term() + " < " + lastTerm());
        }
        entries.add(entry);
    }

    /** Deletes the entry at {@code index} and everything after it. */
    public void truncateFrom(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalStateException("cannot truncate compacted index " + index);
        }
        if (index > lastIndex()) {
            return;
        }
        entries.subList((int) (index - snapshotIndex - 1), entries.size()).clear();
    }

    /** Index of the last retained entry with the given term, or -1. */
    public long lastIndexOfTerm(long term) {
        for (int i = entries.size() - 1; i >= 0; i--) {
            long t = entries.get(i).term();
            if (t == term) {
                return snapshotIndex + i + 1;
            }
            if (t < term) {
                break;
            }
        }
        return -1;
    }

    /** Index of the first retained entry in the run of {@code term} that contains {@code index}. */
    public long firstIndexOfTermRun(long index) {
        long term = termAt(index);
        long i = index;
        while (i - 1 > snapshotIndex && termAt(i - 1) == term) {
            i--;
        }
        return i;
    }
}
