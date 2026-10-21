package io.github.avohra25.jraft.check;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks that a key-value history is linearizable, in the style of Wing &amp; Gong with memoization
 * (as in Lowe's and Knossos/Porcupine's checkers).
 *
 * <p>Keys are independent registers, so each key is checked on its own (linearizability is
 * compositional). For one key we search for a total order of the operations that (a) respects
 * real time: an operation can go next only if it was invoked before every remaining operation
 * returned, and (b) replays correctly against a sequential register: every GET returns the value
 * produced by the operations ordered before it. Operations that never completed may take effect
 * at any point after their invocation, or not at all. The search memoizes (linearized set, state)
 * pairs it has already explored, which keeps it fast for the small client concurrency used here.
 */
public final class LinearizabilityChecker {

    public record Result(boolean linearizable, String key, String explanation) {
        static Result ok() {
            return new Result(true, null, "linearizable");
        }
    }

    private LinearizabilityChecker() {}

    public static Result check(History history) {
        for (Map.Entry<String, List<History.Op>> group : history.byKey().entrySet()) {
            List<History.Op> ops = group.getValue().stream()
                    // A GET that never returned observed nothing and constrains nothing.
                    .filter(op -> op.completed() || op.kind() != History.Kind.GET)
                    .toList();
            if (!new KeySearch(ops).run()) {
                return new Result(false, group.getKey(),
                        "no valid linearization for key " + group.getKey() + " over " + ops.size() + " operations");
            }
        }
        return Result.ok();
    }

    private static final class KeySearch {
        private record Visited(BitSet done, String state) {}

        private final List<History.Op> ops;
        private final int completed;
        private final Set<Visited> visited = new HashSet<>();

        KeySearch(List<History.Op> ops) {
            this.ops = new ArrayList<>(ops);
            this.completed = (int) ops.stream().filter(History.Op::completed).count();
        }

        boolean run() {
            return search(new BitSet(ops.size()), 0, null);
        }

        private boolean search(BitSet done, int completedDone, String state) {
            if (completedDone == completed) {
                return true; // any still-pending writes can be assumed to never take effect
            }
            long horizon = Long.MAX_VALUE;
            for (int i = done.nextClearBit(0); i < ops.size(); i = done.nextClearBit(i + 1)) {
                horizon = Math.min(horizon, ops.get(i).completedAt());
            }
            for (int i = done.nextClearBit(0); i < ops.size(); i = done.nextClearBit(i + 1)) {
                History.Op op = ops.get(i);
                if (op.invokedAt() > horizon) {
                    break; // ops are in invocation order, so no later op can go next either
                }
                Step step = apply(op, state);
                if (!step.valid()) {
                    continue;
                }
                done.set(i);
                if (visited.add(new Visited((BitSet) done.clone(), step.state()))
                        && search(done, completedDone + (op.completed() ? 1 : 0), step.state())) {
                    return true;
                }
                done.clear(i);
            }
            return false;
        }

        private record Step(boolean valid, String state) {}

        private static Step apply(History.Op op, String state) {
            return switch (op.kind()) {
                case PUT -> new Step(true, op.value());
                case APPEND -> new Step(true, (state == null ? "" : state) + op.value());
                case DELETE -> new Step(true, null);
                case GET -> new Step(Objects.equals(op.output(), state), state);
            };
        }
    }
}
