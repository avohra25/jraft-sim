package io.github.avohra25.jraft.check;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A client-observed history: when each operation was invoked, when (if ever) it completed, and
 * what it returned. This is the input to {@link LinearizabilityChecker}.
 */
public final class History {

    public enum Kind { PUT, APPEND, DELETE, GET }

    /** One logical operation; client retries of the same operation share one record. */
    public static final class Op {
        private final int clientId;
        private final long seq;
        private final Kind kind;
        private final String key;
        private final String value;
        private final long invokedAt;
        private long completedAt = -1;
        private String output;

        Op(int clientId, long seq, Kind kind, String key, String value, long invokedAt) {
            this.clientId = clientId;
            this.seq = seq;
            this.kind = kind;
            this.key = key;
            this.value = value;
            this.invokedAt = invokedAt;
        }

        public int clientId() {
            return clientId;
        }

        public long seq() {
            return seq;
        }

        public Kind kind() {
            return kind;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public long invokedAt() {
            return invokedAt;
        }

        public boolean completed() {
            return completedAt >= 0;
        }

        /** Completion time, or {@link Long#MAX_VALUE} for an operation that never returned. */
        public long completedAt() {
            return completed() ? completedAt : Long.MAX_VALUE;
        }

        public String output() {
            return output;
        }

        @Override
        public String toString() {
            String args = switch (kind) {
                case PUT, APPEND -> key + "=" + value;
                case DELETE, GET -> key;
            };
            String result = completed() ? " -> " + output + " @" + completedAt : " (pending)";
            return "c" + clientId + "#" + seq + " " + kind + " " + args + " @" + invokedAt + result;
        }
    }

    private final List<Op> ops = new ArrayList<>();

    public Op invoke(int clientId, long seq, Kind kind, String key, String value, long now) {
        Op op = new Op(clientId, seq, kind, key, value, now);
        ops.add(op);
        return op;
    }

    public void complete(Op op, long now, String output) {
        if (op.completed()) {
            throw new IllegalStateException("operation completed twice: " + op);
        }
        if (now < op.invokedAt) {
            throw new IllegalArgumentException("completion before invocation: " + op);
        }
        op.completedAt = now;
        op.output = output;
    }

    public List<Op> ops() {
        return Collections.unmodifiableList(ops);
    }

    public long completedCount() {
        return ops.stream().filter(Op::completed).count();
    }

    public long pendingCount() {
        return ops.size() - completedCount();
    }

    /** Operations grouped by key, each group in invocation order. */
    public Map<String, List<Op>> byKey() {
        Map<String, List<Op>> grouped = new TreeMap<>();
        for (Op op : ops) {
            grouped.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        return grouped;
    }
}
