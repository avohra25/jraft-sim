package io.github.avohra25.jraft.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.raft.RaftConfig;

/** The whole point of the simulator: a seed is a complete, replayable description of a run. */
class DeterminismTest {

    private static String run(long seed) {
        Cluster cluster = Cluster.builder()
                .seed(seed)
                .raft(RaftConfig.defaults().withSnapshotThreshold(25))
                .traceClients(true)
                .build();
        cluster.start();
        cluster.startWorkload(3_000);
        new Nemesis(cluster, 100, 500).start(3_000);
        cluster.runUntil(3_000);
        cluster.awaitClientsIdle(10_000);
        return cluster.timeline().render() + cluster.history().ops() + cluster.sim().eventsExecuted();
    }

    @Test
    void sameSeedReplaysIdentically() {
        assertEquals(run(77), run(77));
    }

    @Test
    void differentSeedsExploreDifferentRuns() {
        assertNotEquals(run(77), run(78));
    }
}
