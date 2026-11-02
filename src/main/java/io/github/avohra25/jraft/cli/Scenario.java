package io.github.avohra25.jraft.cli;

import java.util.List;

import io.github.avohra25.jraft.check.LinearizabilityChecker;
import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.harness.Figure8;
import io.github.avohra25.jraft.harness.Nemesis;
import io.github.avohra25.jraft.raft.RaftConfig;
import io.github.avohra25.jraft.raft.RaftNode;
import io.github.avohra25.jraft.sim.Network;

/** Canned scenarios for the CLI. Each one drives a cluster and leaves its timeline behind. */
public enum Scenario {

    ELECTION("elect a leader, crash it, watch a new one take over, restart the old one") {
        @Override
        void drive(Cluster cluster, Options options) {
            RaftNode first = cluster.awaitLeader(5_000);
            cluster.runFor(500);
            cluster.crash(first.id());
            cluster.awaitLeader(5_000);
            cluster.runFor(500);
            cluster.restart(first.id());
            cluster.runFor(1_000);
        }
    },

    PARTITION("isolate the leader in a minority while clients write, then heal") {
        @Override
        void drive(Cluster cluster, Options options) {
            cluster.startWorkload(options.duration());
            RaftNode leader = cluster.awaitLeader(5_000);
            cluster.runFor(400);
            int n = cluster.nodes().size();
            List<Integer> minority = new java.util.ArrayList<>(List.of(leader.id()));
            List<Integer> majority = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (i == leader.id()) {
                    continue;
                }
                (minority.size() < (n - 1) / 2 ? minority : majority).add(i);
            }
            cluster.partition(List.of(minority, majority));
            cluster.runFor(Math.max(800, options.duration() / 3));
            cluster.heal();
            cluster.runUntil(options.duration());
        }
    },

    CRASH("crash and restart followers and the leader under load; state comes back from disk") {
        @Override
        void drive(Cluster cluster, Options options) {
            cluster.startWorkload(options.duration());
            int n = cluster.nodes().size();
            long step = Math.max(300, options.duration() / (2L * n));
            for (int round = 0; round < n && cluster.sim().now() + 2 * step < options.duration(); round++) {
                int victim = round % 2 == 0 ? cluster.awaitLeader(5_000).id() : (round * 2) % n;
                cluster.crash(victim);
                cluster.runFor(step);
                cluster.restart(victim);
                cluster.runFor(step);
            }
            cluster.runUntil(options.duration());
        }
    },

    SNAPSHOT("keep a follower down while the log is compacted; it catches up via InstallSnapshot") {
        @Override
        RaftConfig raftConfig() {
            return RaftConfig.defaults().withSnapshotThreshold(25);
        }

        @Override
        void drive(Cluster cluster, Options options) {
            cluster.startWorkload(options.duration());
            RaftNode leader = cluster.awaitLeader(5_000);
            int lagger = (leader.id() + 1) % cluster.nodes().size();
            cluster.runFor(200);
            cluster.crash(lagger);
            cluster.runFor(Math.max(1_500, options.duration() / 2));
            cluster.restart(lagger);
            cluster.runUntil(options.duration());
        }
    },

    FIGURE8("replay Figure 8 of the Raft paper: an old-term entry on a majority that must not count as committed") {
        @Override
        RaftConfig raftConfig() {
            return Figure8.config(RaftConfig.defaults());
        }

        @Override
        Network.Config networkConfig() {
            return Figure8.network();
        }

        @Override
        int nodes(int requested) {
            return Figure8.NODES;
        }

        @Override
        int clients() {
            return 0;
        }

        @Override
        void drive(Cluster cluster, Options options) {
            Figure8.Outcome outcome = Figure8.drive(cluster);
            cluster.timeline().add(cluster.sim().now(), "sim", String.format(
                    "old entry was on a majority: %s; n0 had committed through index %d (a term-%d entry); "
                            + "n4 elected with a divergent log: %s",
                    outcome.oldEntryOnMajority() ? "yes" : "no", outcome.n0CommitIndex(),
                    outcome.committedThroughTerm(), outcome.staleLeaderElected() ? "YES" : "no"));
        }
    },

    CHAOS("random crashes, restarts, partitions and leader isolation on a lossy network") {
        @Override
        RaftConfig raftConfig() {
            return RaftConfig.defaults().withSnapshotThreshold(40);
        }

        @Override
        void drive(Cluster cluster, Options options) {
            cluster.startWorkload(options.duration());
            new Nemesis(cluster, 150, 700).start(options.duration());
            cluster.runUntil(options.duration());
        }
    };

    /**
     * {@code unsafeCommit} turns on the Figure 8 commit bug, to watch the checkers catch it;
     * {@code preVote} and {@code checkQuorum} switch on those election refinements.
     */
    record Options(long seed, int nodes, long duration, boolean verbose, boolean unsafeCommit,
                   boolean preVote, boolean checkQuorum) {

        Options(long seed, int nodes, long duration, boolean verbose) {
            this(seed, nodes, duration, verbose, false, false, false);
        }
    }

    private final String description;

    Scenario(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }

    RaftConfig raftConfig() {
        return RaftConfig.defaults();
    }

    Network.Config networkConfig() {
        return Network.Config.lossy();
    }

    /** The cluster size this scenario runs with when {@code requested} nodes were asked for. */
    int nodes(int requested) {
        return requested;
    }

    int clients() {
        return 3;
    }

    abstract void drive(Cluster cluster, Options options);

    public String cliName() {
        return name().toLowerCase();
    }

    /** Builds a cluster, runs the scenario plus a quiet recovery period, and reports on it. */
    Report run(Options options) {
        Cluster cluster = Cluster.builder()
                .nodes(nodes(options.nodes()))
                .seed(options.seed())
                .network(networkConfig())
                .raft(raftConfig().withUnsafeCommitRule(options.unsafeCommit())
                        .withPreVote(options.preVote()).withCheckQuorum(options.checkQuorum()))
                .clients(clients())
                .traceClients(options.verbose())
                .build();
        cluster.start();
        drive(cluster, options);
        cluster.heal();
        cluster.nodes().forEach(n -> cluster.restart(n.id()));
        boolean idle = cluster.awaitClientsIdle(10_000);
        cluster.runFor(1_000);
        cluster.awaitConvergence(3_000);
        cluster.checker().checkAll();
        return new Report(cluster, idle, LinearizabilityChecker.check(cluster.history()));
    }

    record Report(Cluster cluster, boolean clientsFinished, LinearizabilityChecker.Result linearizability) {
        boolean ok() {
            return cluster.checker().ok() && clientsFinished && linearizability.linearizable() && cluster.converged();
        }
    }
}
