package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.kv.Command;

class RaftLogTest {

    private static LogEntry e(long term, long index) {
        return new LogEntry(term, index, new Command.Noop());
    }

    private static RaftLog logWithTerms(long... terms) {
        RaftLog log = new RaftLog();
        for (int i = 0; i < terms.length; i++) {
            log.append(e(terms[i], i + 1));
        }
        return log;
    }

    @Test
    void emptyLogHasSentinelAtIndexZero() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.termAt(0));
        assertTrue(log.hasTermAt(0));
        assertFalse(log.hasTermAt(1));
    }

    @Test
    void appendRequiresContiguousIndexesAndMonotonicTerms() {
        RaftLog log = logWithTerms(1, 1, 2);
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
        assertThrows(IllegalArgumentException.class, () -> log.append(e(2, 5)));
        assertThrows(IllegalArgumentException.class, () -> log.append(e(1, 4)));
    }

    @Test
    void truncateAndSlice() {
        RaftLog log = logWithTerms(1, 1, 2, 3, 3);
        assertEquals(List.of(e(2, 3), e(3, 4)), log.slice(3, 2));
        assertEquals(List.of(), log.slice(6, 10));
        log.truncateFrom(4);
        assertEquals(3, log.lastIndex());
        log.truncateFrom(10); // no-op
        assertEquals(3, log.lastIndex());
    }

    @Test
    void termSearchHelpers() {
        RaftLog log = logWithTerms(1, 1, 2, 2, 2, 4);
        assertEquals(5, log.lastIndexOfTerm(2));
        assertEquals(-1, log.lastIndexOfTerm(3));
        assertEquals(3, log.firstIndexOfTermRun(5));
        assertEquals(1, log.firstIndexOfTermRun(2));
    }

    @Test
    void outOfRangeAccessFails() {
        RaftLog log = logWithTerms(1);
        assertThrows(IndexOutOfBoundsException.class, () -> log.entry(0));
        assertThrows(IndexOutOfBoundsException.class, () -> log.entry(2));
    }
}
