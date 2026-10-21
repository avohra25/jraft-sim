package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.harness.Cluster;

class SnapshotTest {

    private static Cluster started(long seed, int threshold) {
        Cluster cluster = Cluster.builder()
                .nodes(5)
                .seed(seed)
                .clients(3)
                .raft(RaftConfig.defaults().withSnapshotThreshold(threshold))
                .build();
        cluster.start();
        return cluster;
    }

    private static boolean timelineMentions(Cluster cluster, String who, String text) {
        return cluster.timeline().events().stream().anyMatch(e -> e.who().equals(who) && e.what().contains(text));
    }

    @Test
    void logsStayBoundedOnceSnapshotsKickIn() {
        Cluster cluster = started(1, 20);
        cluster.startWorkload(4_000);
        cluster.runUntil(4_000);
        assertTrue(cluster.awaitClientsIdle(10_000));
        cluster.runFor(1_000);
        assertTrue(cluster.awaitConvergence(3_000));
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
        for (RaftNode node : cluster.nodes()) {
            assertTrue(node.log().snapshotIndex() > 0, node + " never compacted");
            // Entries are compacted once `threshold` applied ones pile up; uncommitted ones may follow.
            assertTrue(node.log().size() <= 20 + 64, node + " retains " + node.log().size() + " entries");
        }
    }

    @Test
    void laggingFollowerCatchesUpThroughInstallSnapshot() {
        Cluster cluster = started(2, 15);
        RaftNode leader = cluster.awaitLeader(3_000);
        int lagger = (leader.id() + 1) % 5;
        cluster.crash(lagger);
        cluster.startWorkload(3_000);
        cluster.runUntil(3_000);
        RaftNode current = cluster.leader().orElseThrow();
        assertTrue(current.log().snapshotIndex() > cluster.node(lagger).log().lastIndex(),
                "the entries the follower needs must already be compacted away");

        cluster.restart(lagger);
        assertTrue(cluster.awaitClientsIdle(10_000));
        cluster.runFor(1_000);
        assertTrue(timelineMentions(cluster, "n" + lagger, "installed snapshot"));
        assertTrue(cluster.awaitConvergence(3_000));
        assertEquals(cluster.kv(current.id()).data(), cluster.kv(lagger).data());
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
    }

    @Test
    void restartRestoresStateFromTheLocalSnapshot() {
        Cluster cluster = started(3, 10);
        cluster.startWorkload(2_000);
        cluster.runUntil(2_000);
        assertTrue(cluster.awaitClientsIdle(10_000));
        cluster.runFor(500);
        RaftNode node = cluster.node(4);
        long snapshotIndex = node.log().snapshotIndex();
        assertTrue(snapshotIndex > 0);

        cluster.crash(4);
        cluster.restart(4);
        assertEquals(snapshotIndex, node.commitIndex(), "a restarted node knows its snapshot is committed");
        assertEquals(snapshotIndex, node.lastApplied());
        assertFalse(cluster.kv(4).data().isEmpty(), "state machine restored before hearing from anyone");
        cluster.runFor(1_000);
        assertTrue(cluster.awaitConvergence(3_000));
    }

    @Test
    void installSnapshotOverwritesADivergentLog() {
        // A follower partitioned away while leader it followed kept accepting (doomed) writes would
        // hold a conflicting suffix; after compaction elsewhere it must take the snapshot wholesale.
        Cluster cluster = started(4, 10);
        cluster.startWorkload(6_000);
        RaftNode old = cluster.awaitLeader(3_000);
        cluster.runFor(300);
        java.util.List<Integer> rest = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            if (i != old.id()) {
                rest.add(i);
            }
        }
        cluster.partition(java.util.List.of(java.util.List.of(old.id()), rest));
        cluster.runFor(3_000);
        cluster.heal();
        cluster.runUntil(6_000);
        assertTrue(cluster.awaitClientsIdle(10_000));
        cluster.runFor(1_000);
        assertTrue(cluster.awaitConvergence(3_000));
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
    }
}
