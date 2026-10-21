package io.github.avohra25.jraft.harness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import io.github.avohra25.jraft.check.InvariantChecker;
import io.github.avohra25.jraft.check.History;
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
 * A simulated Raft cluster: nodes, their disks, the network, clients and observers, all driven by
 * one seeded {@link Simulator}. Node ids are {@code 0..n-1}; client ids start at {@link #CLIENT_BASE}.
 */
public final class Cluster implements RaftListener {

    public static final int CLIENT_BASE = 100;
    private static final long CHECK_INTERVAL = 100;

    public static final class Builder {
        private int nodes = 5;
        private long seed = 1;
        private Network.Config network = Network.Config.lossy();
        private RaftConfig raft = RaftConfig.defaults();
        private int clients = 3;
        private int keys = 3;
        private boolean traceClients;

        public Builder nodes(int nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder network(Network.Config network) {
            this.network = network;
            return this;
        }

        public Builder raft(RaftConfig raft) {
            this.raft = raft;
            return this;
        }

        public Builder clients(int clients) {
            this.clients = clients;
            return this;
        }

        public Builder keys(int keys) {
            this.keys = keys;
            return this;
        }

        /** Also put every completed client operation on the timeline. */
        public Builder traceClients(boolean trace) {
            this.traceClients = trace;
            return this;
        }

        public Cluster build() {
            if (nodes < 1 || clients < 0 || keys < 1) {
                throw new IllegalArgumentException("need at least one node and one key");
            }
            return new Cluster(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final Simulator sim;
    private final Network<Message> network;
    private final List<PersistentState> disks = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<KvClient> clients = new ArrayList<>();
    private final History history = new History();
    private final Timeline timeline = new Timeline();
    private final InvariantChecker checker;

    private Cluster(Builder b) {
        sim = new Simulator(b.seed);
        network = new Network<>(sim, b.network);
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < b.nodes; i++) {
            ids.add(i);
        }
        checker = new InvariantChecker(this::nodes, sim::now);
        for (int i = 0; i < b.nodes; i++) {
            int self = i;
            List<Integer> peers = ids.stream().filter(p -> p != self).toList();
            PersistentState disk = new PersistentState();
            disks.add(disk);
            nodes.add(new RaftNode(i, peers, sim, network, b.raft, disk, KvStore::new, this));
        }
        List<String> keys = new ArrayList<>();
        for (int k = 0; k < b.keys; k++) {
            keys.add("k" + k);
        }
        for (int c = 0; c < b.clients; c++) {
            clients.add(new KvClient(CLIENT_BASE + c, sim, network, ids, keys, history,
                    b.traceClients ? timeline : null));
        }
    }

    // ---------------------------------------------------------------- driving the simulation

    /** Boots every node. */
    public void start() {
        timeline.add(sim.now(), "sim", "cluster of " + nodes.size() + " nodes started (seed " + sim.seed() + ")");
        nodes.forEach(RaftNode::start);
        scheduleChecks();
    }

    /** Lets every client issue operations until simulated time {@code until}. */
    public void startWorkload(long until) {
        clients.forEach(c -> c.start(until));
    }

    public void runFor(long millis) {
        sim.runFor(millis);
    }

    public void runUntil(long time) {
        sim.runUntil(time);
    }

    /** Runs until a leader exists, failing after {@code timeout} simulated ms. */
    public RaftNode awaitLeader(long timeout) {
        long deadline = sim.now() + timeout;
        while (sim.now() < deadline) {
            Optional<RaftNode> leader = leader();
            if (leader.isPresent()) {
                return leader.get();
            }
            sim.runFor(5);
        }
        throw new IllegalStateException("no leader within " + timeout + "ms: " + nodes);
    }

    /** Runs until every client is idle, giving up after {@code timeout} simulated ms. */
    public boolean awaitClientsIdle(long timeout) {
        long deadline = sim.now() + timeout;
        while (clients.stream().anyMatch(KvClient::busy)) {
            if (sim.now() >= deadline) {
                return false;
            }
            sim.runFor(10);
        }
        return true;
    }

    /**
     * Runs until {@link #converged()} holds, giving up after {@code timeout} simulated ms. Even a
     * healthy cluster is only converged between commits (a lossy network can also trigger a
     * spontaneous election whose no-op is briefly committed on the leader alone), so tests wait
     * for convergence rather than sampling it at one instant.
     */
    public boolean awaitConvergence(long timeout) {
        long deadline = sim.now() + timeout;
        while (!converged()) {
            if (sim.now() >= deadline) {
                return false;
            }
            sim.runFor(10);
        }
        return true;
    }

    private void scheduleChecks() {
        sim.schedule(CHECK_INTERVAL, () -> {
            checker.checkAll();
            scheduleChecks();
        });
    }

    // ---------------------------------------------------------------- faults

    public void crash(int id) {
        RaftNode node = nodes.get(id);
        if (node.isUp()) {
            node.crash();
            timeline.add(sim.now(), "n" + id, "CRASHED");
        }
    }

    public void restart(int id) {
        RaftNode node = nodes.get(id);
        if (!node.isUp()) {
            node.start();
            timeline.add(sim.now(), "n" + id, "restarted from disk: term " + node.currentTerm()
                    + ", voted " + (node.votedFor() < 0 ? "-" : "n" + node.votedFor())
                    + ", log " + node.log().firstIndex() + ".." + node.log().lastIndex());
        }
    }

    /** Splits the nodes into groups that cannot reach each other (clients still reach everyone). */
    public void partition(List<List<Integer>> groups) {
        network.partition(groups);
        timeline.add(sim.now(), "net", "PARTITION " + groups.stream()
                .map(g -> g.stream().map(i -> "n" + i).collect(Collectors.joining(",", "{", "}")))
                .collect(Collectors.joining(" | ")));
    }

    public void isolate(int id) {
        network.isolate(id);
        timeline.add(sim.now(), "net", "ISOLATED n" + id);
    }

    public void heal() {
        if (network.isPartitioned()) {
            network.heal();
            timeline.add(sim.now(), "net", "HEALED");
        }
    }

    // ---------------------------------------------------------------- inspection

    public Simulator sim() {
        return sim;
    }

    public Network<Message> network() {
        return network;
    }

    public List<RaftNode> nodes() {
        return Collections.unmodifiableList(nodes);
    }

    public RaftNode node(int id) {
        return nodes.get(id);
    }

    public List<KvClient> clients() {
        return Collections.unmodifiableList(clients);
    }

    public History history() {
        return history;
    }

    public Timeline timeline() {
        return timeline;
    }

    public InvariantChecker checker() {
        return checker;
    }

    /** The running leader with the highest term, if any. */
    public Optional<RaftNode> leader() {
        return nodes.stream()
                .filter(n -> n.isUp() && n.role() == RaftNode.Role.LEADER)
                .max((a, b) -> Long.compare(a.currentTerm(), b.currentTerm()));
    }

    public KvStore kv(int id) {
        return (KvStore) nodes.get(id).stateMachine();
    }

    /** True if every running node has applied the same prefix and holds identical data. */
    public boolean converged() {
        List<RaftNode> running = nodes.stream().filter(RaftNode::isUp).toList();
        if (running.isEmpty()) {
            return true;
        }
        RaftNode first = running.getFirst();
        return running.stream().allMatch(n -> n.lastApplied() == first.lastApplied()
                && n.commitIndex() == first.commitIndex()
                && Objects.equals(kv(n.id()).data(), kv(first.id()).data()));
    }

    // ---------------------------------------------------------------- RaftListener

    @Override
    public void onRoleChange(RaftNode node, RaftNode.Role previous) {
        String who = "n" + node.id();
        if (!node.isUp()) {
            return; // the crash itself is already on the timeline
        }
        switch (node.role()) {
            case CANDIDATE -> timeline.add(sim.now(), who, "election timeout -> CANDIDATE for term " + node.currentTerm());
            case LEADER -> {
                timeline.add(sim.now(), who, "became LEADER for term " + node.currentTerm()
                        + " (log last index " + node.log().lastIndex() + ")");
                checker.onBecameLeader(node);
            }
            case FOLLOWER -> timeline.add(sim.now(), who, previous + " -> FOLLOWER, term " + node.currentTerm());
        }
    }

    @Override
    public void onLeaderCommit(RaftNode leader, long commitIndex) {
        checker.onLeaderCommit(leader, commitIndex);
    }

    @Override
    public void onApply(RaftNode node, LogEntry entry, String result) {
        checker.onApply(node, entry);
    }

    @Override
    public void onSnapshot(RaftNode node, long lastIncludedIndex, boolean installed) {
        timeline.add(sim.now(), "n" + node.id(), installed
                ? "installed snapshot from leader through index " + lastIncludedIndex
                : "compacted log into snapshot through index " + lastIncludedIndex);
    }
}
