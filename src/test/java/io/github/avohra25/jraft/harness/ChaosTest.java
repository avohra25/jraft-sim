package io.github.avohra25.jraft.harness;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.stream.LongStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.avohra25.jraft.check.LinearizabilityChecker;
import io.github.avohra25.jraft.raft.RaftConfig;
import io.github.avohra25.jraft.sim.Network;

/**
 * Randomized fault-injection runs: every seed is a different interleaving of crashes, restarts,
 * partitions, drops, duplicates and reorderings. Each run must keep every safety invariant, give
 * linearizable client results, and recover (finish all client operations and converge) once the
 * faults stop. A failure message names the seed, which replays the run exactly.
 *
 * <p>Runs take a few milliseconds each. Scale the sweep with {@code -Dchaos.seeds=N} (default
 * 1000 five-node seeds plus half as many three-node seeds); rare schedules such as the paper's
 * Figure 8 only show up with enough seeds.
 */
class ChaosTest {

    private static final int SEEDS = Integer.getInteger("chaos.seeds", 1_000);

    static LongStream fiveNodeSeeds() {
        return LongStream.rangeClosed(1, SEEDS);
    }

    static LongStream threeNodeSeeds() {
        return LongStream.rangeClosed(1_000_001, 1_000_000 + SEEDS / 2);
    }

    @ParameterizedTest(name = "5 nodes, seed {0}")
    @MethodSource("fiveNodeSeeds")
    void fiveNodes(long seed) {
        run(seed, 5);
    }

    @ParameterizedTest(name = "3 nodes, seed {0}")
    @MethodSource("threeNodeSeeds")
    void threeNodes(long seed) {
        run(seed, 3);
    }

    static void run(long seed, int nodes) {
        Cluster cluster = Cluster.builder()
                .nodes(nodes)
                .seed(seed)
                .network(Network.Config.lossy())
                .raft(RaftConfig.defaults().withSnapshotThreshold(30))
                .clients(3)
                .keys(3)
                .build();
        Nemesis nemesis = new Nemesis(cluster, 100, 600);
        cluster.start();
        cluster.startWorkload(6_000);
        nemesis.start(6_000);
        cluster.runUntil(6_000);

        String where = "seed " + seed + " (" + nodes + " nodes)";
        if (!cluster.awaitClientsIdle(15_000)) {
            fail(where + ": clients did not finish after recovery: " + cluster.nodes());
        }
        cluster.runFor(1_500);
        cluster.checker().checkAll();

        assertTrue(cluster.checker().ok(), where + ": " + cluster.checker().violations());
        LinearizabilityChecker.Result lin = LinearizabilityChecker.check(cluster.history());
        assertTrue(lin.linearizable(), where + ": " + lin.explanation());
        assertTrue(cluster.awaitConvergence(3_000), where + ": replicas did not converge: " + cluster.nodes());
        assertTrue(nemesis.faultsInjected() > 0, where + ": nemesis idle");
        assertTrue(cluster.checker().committedEntries() > 0, where + ": nothing committed");
    }
}
