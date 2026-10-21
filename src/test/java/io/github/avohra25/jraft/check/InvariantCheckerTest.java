package io.github.avohra25.jraft.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.kv.Command;
import io.github.avohra25.jraft.kv.KvStore;
import io.github.avohra25.jraft.raft.LogEntry;
import io.github.avohra25.jraft.raft.Message;
import io.github.avohra25.jraft.raft.PersistentState;
import io.github.avohra25.jraft.raft.RaftConfig;
import io.github.avohra25.jraft.raft.RaftListener;
import io.github.avohra25.jraft.raft.RaftNode;
import io.github.avohra25.jraft.sim.Network;
import io.github.avohra25.jraft.sim.Simulator;

/**
 * The checker must not be vacuous: feed it hand-built states that break each invariant and make
 * sure it notices. Nodes here are never started; only their durable state is used.
 */
class InvariantCheckerTest {

    private final List<PersistentState> disks = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();
    private InvariantChecker checker;

    @BeforeEach
    void setUp() {
        Simulator sim = new Simulator(1);
        Network<Message> net = new Network<>(sim, Network.Config.reliable());
        for (int i = 0; i < 3; i++) {
            int self = i;
            PersistentState disk = new PersistentState();
            disks.add(disk);
            List<Integer> peers = List.of(0, 1, 2).stream().filter(p -> p != self).toList();
            nodes.add(new RaftNode(i, peers, sim, net, RaftConfig.defaults(), disk, KvStore::new, new RaftListener() { }));
        }
        checker = new InvariantChecker(() -> nodes, sim::now);
    }

    private static LogEntry put(long term, long index, String value) {
        return new LogEntry(term, index, new Command.Put(1, index, "k", value));
    }

    private void append(int node, LogEntry... entries) {
        for (LogEntry e : entries) {
            disks.get(node).log().append(e);
        }
    }

    @Test
    void consistentLogsPass() {
        append(0, put(1, 1, "a"), put(1, 2, "b"), put(2, 3, "c"));
        append(1, put(1, 1, "a"), put(1, 2, "b"), put(3, 3, "x"));
        append(2, put(1, 1, "a"));
        checker.checkLogMatching();
        assertTrue(checker.ok(), checker.violations().toString());
        assertTrue(checker.checks() > 0);
    }

    @Test
    void detectsLogMatchingViolation() {
        append(0, put(1, 1, "a"), put(2, 2, "b"));
        append(1, put(1, 1, "DIFFERENT"), put(2, 2, "b"));
        checker.checkLogMatching();
        assertEquals(1, checker.violations().size());
        assertTrue(checker.violations().getFirst().contains("log matching"));
    }

    @Test
    void detectsTwoLeadersInOneTerm() {
        disks.get(0).advanceTerm(4);
        disks.get(1).advanceTerm(4);
        checker.onBecameLeader(nodes.get(0));
        checker.onBecameLeader(nodes.get(0)); // the same leader twice is fine
        assertTrue(checker.ok());
        checker.onBecameLeader(nodes.get(1));
        assertTrue(checker.violations().getFirst().contains("election safety"));
    }

    @Test
    void detectsStateMachineSafetyViolations() {
        disks.get(0).advanceTerm(1);
        append(0, put(1, 1, "a"));
        checker.onLeaderCommit(nodes.get(0), 1);
        checker.onApply(nodes.get(0), put(1, 1, "a"));
        assertTrue(checker.ok(), checker.violations().toString());

        checker.onApply(nodes.get(1), put(1, 1, "b"));
        assertTrue(checker.violations().stream().anyMatch(v -> v.contains("state machine safety")));
        checker.onApply(nodes.get(2), put(1, 2, "z"));
        assertTrue(checker.violations().stream().anyMatch(v -> v.contains("before any leader committed")));
    }

    @Test
    void detectsLeaderMissingACommittedEntry() {
        disks.get(0).advanceTerm(1);
        append(0, put(1, 1, "a"));
        checker.onLeaderCommit(nodes.get(0), 1);
        disks.get(1).advanceTerm(2); // node 1 has an empty log but becomes leader of term 2
        checker.onBecameLeader(nodes.get(1));
        assertEquals(1, checker.violations().size());
        assertTrue(checker.violations().getFirst().contains("leader completeness"));
    }

    @Test
    void detectsACommittedEntryChanging() {
        disks.get(0).advanceTerm(1);
        append(0, put(1, 1, "a"));
        checker.onLeaderCommit(nodes.get(0), 1);
        disks.get(1).advanceTerm(2);
        append(1, put(2, 1, "b"));
        checker.onLeaderCommit(nodes.get(1), 1);
        assertTrue(checker.violations().getFirst().contains("committed entry changed"));
    }
}
