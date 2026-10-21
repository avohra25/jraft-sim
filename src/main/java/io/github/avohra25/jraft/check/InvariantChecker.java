package io.github.avohra25.jraft.check;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import io.github.avohra25.jraft.raft.LogEntry;
import io.github.avohra25.jraft.raft.RaftLog;
import io.github.avohra25.jraft.raft.RaftNode;

/**
 * Online checker for the safety properties of Figure 3 in the Raft paper.
 *
 * <ul>
 *   <li><b>Election safety</b>: at most one leader per term.</li>
 *   <li><b>Log matching</b>: if two logs hold an entry with the same index and term, the logs are
 *       identical up to that index.</li>
 *   <li><b>Leader completeness</b>: an entry committed in term T is present in the log of every
 *       leader of a term greater than T.</li>
 *   <li><b>State machine safety</b>: no two servers apply different entries at the same index,
 *       and nothing is applied before it is committed.</li>
 * </ul>
 *
 * <p>The checker is fed by {@link io.github.avohra25.jraft.raft.RaftListener} callbacks and by
 * periodic {@link #checkLogMatching()} sweeps. It records violations instead of throwing, so a
 * test can report all of them along with the seed that produced them.
 */
public final class InvariantChecker {

    private record Committed(LogEntry entry, long commitTerm) {}

    private static final int MAX_RECORDED = 20;

    private final Supplier<List<RaftNode>> nodes;
    private final LongSupplier clock;
    private final List<String> violations = new ArrayList<>();
    private final Map<Long, Integer> leaderByTerm = new HashMap<>();
    private final TreeMap<Long, Committed> committed = new TreeMap<>();
    private final Map<Long, LogEntry> applied = new HashMap<>();
    private final Map<Integer, Long> verifiedThrough = new HashMap<>();
    private long checks;
    private long violationCount;

    public InvariantChecker(Supplier<List<RaftNode>> nodes, LongSupplier clock) {
        this.nodes = nodes;
        this.clock = clock;
    }

    // ---------------------------------------------------------------- hooks

    public void onBecameLeader(RaftNode leader) {
        checks++;
        long term = leader.currentTerm();
        Integer previous = leaderByTerm.putIfAbsent(term, leader.id());
        if (previous != null && previous != leader.id()) {
            violation("election safety: n" + previous + " and n" + leader.id() + " both led term " + term);
        }
        verifiedThrough.remove(leader.id());
        for (Committed c : committed.values()) {
            checkLeaderHas(leader, c);
        }
    }

    public void onLeaderCommit(RaftNode leader, long commitIndex) {
        checks++;
        RaftLog log = leader.log();
        long from = Math.max(log.firstIndex(), verifiedThrough.getOrDefault(leader.id(), 0L) + 1);
        for (long i = from; i <= commitIndex; i++) {
            LogEntry entry = log.entry(i);
            Committed known = committed.get(i);
            if (known == null) {
                Committed fresh = new Committed(entry, leader.currentTerm());
                committed.put(i, fresh);
                for (RaftNode other : nodes.get()) {
                    if (other.isUp() && other.role() == RaftNode.Role.LEADER && other.currentTerm() > fresh.commitTerm()) {
                        checkLeaderHas(other, fresh);
                    }
                }
            } else if (!known.entry().equals(entry)) {
                violation("committed entry changed at index " + i + ": " + known.entry() + " vs " + entry
                        + " on leader n" + leader.id());
            }
        }
        verifiedThrough.put(leader.id(), Math.max(commitIndex, verifiedThrough.getOrDefault(leader.id(), 0L)));
    }

    public void onApply(RaftNode node, LogEntry entry) {
        checks++;
        Committed known = committed.get(entry.index());
        if (known == null) {
            violation("n" + node.id() + " applied index " + entry.index() + " before any leader committed it");
        } else if (!known.entry().equals(entry)) {
            violation("n" + node.id() + " applied " + entry + " but " + known.entry() + " was committed");
        }
        LogEntry first = applied.putIfAbsent(entry.index(), entry);
        if (first != null && !first.equals(entry)) {
            violation("state machine safety: index " + entry.index() + " applied as both " + first + " and " + entry);
        }
    }

    // ---------------------------------------------------------------- sweeps

    /** Pairwise log-matching sweep over every node's durable log (crashed nodes included). */
    public void checkLogMatching() {
        List<RaftNode> all = nodes.get();
        for (int a = 0; a < all.size(); a++) {
            for (int b = a + 1; b < all.size(); b++) {
                checkPair(all.get(a), all.get(b));
            }
        }
    }

    private void checkPair(RaftNode a, RaftNode b) {
        checks++;
        RaftLog x = a.log();
        RaftLog y = b.log();
        long lo = Math.max(x.firstIndex(), y.firstIndex());
        long hi = Math.min(x.lastIndex(), y.lastIndex());
        long agree = -1;
        for (long i = hi; i >= lo; i--) {
            if (x.entry(i).term() == y.entry(i).term()) {
                agree = i;
                break;
            }
        }
        for (long i = lo; i <= agree; i++) {
            if (!x.entry(i).equals(y.entry(i))) {
                violation("log matching: n" + a.id() + " and n" + b.id() + " agree on the term at index " + agree
                        + " but differ at index " + i + ": " + x.entry(i) + " vs " + y.entry(i));
                return;
            }
        }
    }

    /** Leader completeness for every current leader, plus a log-matching sweep. */
    public void checkAll() {
        checkLogMatching();
        for (RaftNode node : nodes.get()) {
            if (node.isUp() && node.role() == RaftNode.Role.LEADER) {
                for (Committed c : committed.values()) {
                    checkLeaderHas(node, c);
                }
            }
        }
    }

    private void checkLeaderHas(RaftNode leader, Committed c) {
        if (leader.currentTerm() <= c.commitTerm()) {
            return;
        }
        RaftLog log = leader.log();
        long index = c.entry().index();
        if (index <= log.snapshotIndex()) {
            return; // folded into a snapshot, which only ever contains committed entries
        }
        if (index > log.lastIndex() || !log.entry(index).equals(c.entry())) {
            violation("leader completeness: n" + leader.id() + " leads term " + leader.currentTerm()
                    + " without " + c.entry() + " committed in term " + c.commitTerm());
        }
    }

    // ---------------------------------------------------------------- results

    private void violation(String message) {
        violationCount++;
        if (violations.size() < MAX_RECORDED) {
            violations.add("t=" + clock.getAsLong() + "ms " + message);
        }
    }

    public List<String> violations() {
        return Collections.unmodifiableList(violations);
    }

    public boolean ok() {
        return violationCount == 0;
    }

    /** Total violations seen; only the first {@value #MAX_RECORDED} are kept as messages. */
    public long violationCount() {
        return violationCount;
    }

    /** Number of individual checks performed, so tests can tell the checker actually ran. */
    public long checks() {
        return checks;
    }

    public int committedEntries() {
        return committed.size();
    }

    public int leadersObserved() {
        return leaderByTerm.size();
    }
}
