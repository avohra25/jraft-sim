package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.kv.Command;

class ReplicationTest {

    private static Cluster started(long seed) {
        Cluster cluster = Cluster.builder().nodes(5).seed(seed).clients(3).build();
        cluster.start();
        return cluster;
    }

    private static void finish(Cluster cluster) {
        assertTrue(cluster.awaitClientsIdle(10_000), "clients never finished");
        cluster.runFor(1_000);
        assertTrue(cluster.awaitConvergence(3_000), "replicas diverged: " + cluster.nodes());
    }

    @Test
    void clientWritesReachEveryReplica() {
        Cluster cluster = started(1);
        cluster.startWorkload(3_000);
        cluster.runUntil(3_000);
        finish(cluster);
        assertTrue(cluster.history().completedCount() > 50, "too few operations: " + cluster.history().completedCount());
        assertEquals(0, cluster.history().pendingCount());
        RaftNode leader = cluster.leader().orElseThrow();
        for (RaftNode node : cluster.nodes()) {
            assertEquals(leader.log().entries(), node.log().entries(), "log of " + node);
            assertFalse(cluster.kv(node.id()).data().isEmpty());
        }
    }

    @Test
    void restartedFollowerReplaysTheLog() {
        Cluster cluster = started(2);
        RaftNode leader = cluster.awaitLeader(3_000);
        int follower = (leader.id() + 2) % 5;
        cluster.crash(follower);
        cluster.startWorkload(2_000);
        cluster.runUntil(2_000);
        assertTrue(cluster.node(follower).log().lastIndex() < leader.log().lastIndex());
        cluster.restart(follower);
        assertEquals(0, cluster.node(follower).commitIndex(), "commit index is volatile");
        finish(cluster);
        assertEquals(leader.commitIndex(), cluster.node(follower).lastApplied());
    }

    @Test
    void uncommittedEntriesOfADeposedLeaderAreOverwritten() {
        Cluster cluster = Cluster.builder().nodes(5).seed(4).clients(0).build();
        cluster.start();
        RaftNode old = cluster.awaitLeader(3_000);
        cluster.runFor(200);
        long committed = old.commitIndex();
        List<Integer> rest = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            if (i != old.id()) {
                rest.add(i);
            }
        }
        cluster.partition(List.of(List.of(old.id()), rest));
        // These reach the stranded leader (999 is outside the partition) but can never commit.
        for (int seq = 1; seq <= 3; seq++) {
            cluster.network().send(999, old.id(), new Message.ClientRequest(new Command.Put(999, seq, "k", "lost" + seq)));
        }
        cluster.runFor(1_500);
        assertEquals(committed + 3, old.log().lastIndex());
        assertEquals(committed, old.commitIndex());

        RaftNode fresh = cluster.leader().orElseThrow();
        cluster.network().send(998, fresh.id(), new Message.ClientRequest(new Command.Put(998, 1, "k", "kept")));
        cluster.runFor(500);
        cluster.heal();
        cluster.runFor(1_000);

        assertEquals(RaftNode.Role.FOLLOWER, old.role());
        assertTrue(cluster.awaitConvergence(3_000));
        assertEquals(fresh.log().entries(), old.log().entries());
        assertEquals("kept", cluster.kv(old.id()).get("k"));
    }

    @Test
    void writesStallWithoutAMajorityAndResumeAfterRecovery() {
        Cluster cluster = started(6);
        cluster.awaitLeader(3_000);
        cluster.crash(0);
        cluster.crash(1);
        cluster.crash(2);
        cluster.startWorkload(1_000);
        cluster.runUntil(3_000);
        long done = cluster.history().completedCount();
        cluster.runFor(2_000);
        assertEquals(done, cluster.history().completedCount(), "progress without a majority");
        assertTrue(cluster.history().pendingCount() > 0);
        cluster.restart(0);
        finish(cluster);
        assertEquals(0, cluster.history().pendingCount());
    }
}
