package io.github.avohra25.jraft.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.check.History.Kind;

class LinearizabilityCheckerTest {

    private final History history = new History();
    private long seq;

    private History.Op op(Kind kind, String key, String value, long start, long end, String output) {
        History.Op op = history.invoke(1, ++seq, kind, key, value, start);
        if (end >= 0) {
            history.complete(op, end, output);
        }
        return op;
    }

    private boolean linearizable() {
        return LinearizabilityChecker.check(history).linearizable();
    }

    @Test
    void sequentialHistory() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.GET, "a", null, 20, 30, "1");
        op(Kind.DELETE, "a", null, 40, 50, "OK");
        op(Kind.GET, "a", null, 60, 70, null);
        assertTrue(linearizable());
    }

    @Test
    void staleReadIsRejected() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.PUT, "a", "2", 20, 30, "OK");
        op(Kind.GET, "a", null, 40, 50, "1");
        LinearizabilityChecker.Result result = LinearizabilityChecker.check(history);
        assertFalse(result.linearizable());
        assertEquals("a", result.key());
    }

    @Test
    void concurrentReadMaySeeEitherValue() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.PUT, "a", "2", 20, 60, "OK");
        op(Kind.GET, "a", null, 30, 40, "1");
        op(Kind.GET, "a", null, 45, 50, "2");
        assertTrue(linearizable());
    }

    @Test
    void readsMustNotGoBackInTime() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.PUT, "a", "2", 20, 100, "OK");
        op(Kind.GET, "a", null, 30, 40, "2");
        op(Kind.GET, "a", null, 50, 60, "1"); // 2 was already visible
        assertFalse(linearizable());
    }

    @Test
    void pendingWriteMayOrMayNotTakeEffect() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.PUT, "a", "2", 20, -1, null); // never returned
        op(Kind.GET, "a", null, 30, 40, "1");
        op(Kind.GET, "a", null, 50, 60, "2");
        assertTrue(linearizable());
        op(Kind.GET, "a", null, 70, 80, "1");
        assertFalse(linearizable());
    }

    @Test
    void concurrentAppendsInEitherOrderButNotLost() {
        op(Kind.APPEND, "a", "x", 0, 10, "OK");
        op(Kind.APPEND, "a", "y", 0, 10, "OK");
        op(Kind.GET, "a", null, 20, 30, "yx");
        assertTrue(linearizable());
        op(Kind.GET, "a", null, 40, 50, "y");
        assertFalse(linearizable());
    }

    @Test
    void keysAreCheckedIndependently() {
        op(Kind.PUT, "a", "1", 0, 10, "OK");
        op(Kind.PUT, "b", "2", 0, 10, "OK");
        op(Kind.GET, "a", null, 20, 30, "1");
        op(Kind.GET, "b", null, 20, 30, "2");
        assertTrue(linearizable());
        op(Kind.GET, "b", null, 40, 50, "1");
        assertEquals("b", LinearizabilityChecker.check(history).key());
    }
}
