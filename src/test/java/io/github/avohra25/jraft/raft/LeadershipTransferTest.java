package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.avohra25.jraft.check.LinearizabilityChecker;
import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.kv.Command;
import io.github.avohra25.jraft.sim.Network;

class LeadershipTransferTest {

    private static Cluster cluster(long seed, RaftConfig raft) {
        Cluster cluster = Cluster.builder().nodes(5).seed(seed).network(Network.Config.reliable())
                .raft(raft).clients(0).build();
        cluster.start();
        return cluster;
    }

    private static void assertHandsOver(Cluster cluster) {
        RaftNode leader = cluster.awaitLeader(3_000);
        cluster.runFor(200);
        long term = leader.currentTerm();
        int target = (leader.id() + 2) % 5;
        long start = cluster.sim().now();
        cluster.transferLeadership(leader.id(), target);
        assertTrue(cluster.stepUntil(() -> cluster.node(target).role() == RaftNode.Role.LEADER, 100),
                "target never took over: " + cluster.nodes());
        assertTrue(cluster.sim().now() - start < RaftConfig.defaults().electionTimeoutMin(),
                "a transfer should be much faster than waiting for a timeout");
        assertEquals(term + 1, cluster.node(target).currentTerm());
        cluster.runFor(500);
        assertEquals(cluster.node(target), cluster.leader().orElseThrow());
        assertEquals(RaftNode.Role.FOLLOWER, leader.role());
        assertEquals(target, leader.leaderId());
        cluster.checker().checkAll();
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void handsLeadershipToAnUpToDateFollower(long seed) {
        assertHandsOver(cluster(seed, RaftConfig.defaults()));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void transferGetsPastPreVoteAndTheLeaderLease(long seed) {
        assertHandsOver(cluster(seed, RaftConfig.defaults().withPreVote(true).withCheckQuorum(true)));
    }

    @Test
    void catchesALaggingTargetUpFirst() {
        Cluster cluster = cluster(7, RaftConfig.defaults().withMaxEntriesPerAppend(2));
        RaftNode leader = cluster.awaitLeader(3_000);
        int target = (leader.id() + 1) % 5;
        cluster.crash(target);
        for (int seq = 1; seq <= 20; seq++) {
            cluster.propose(leader.id(), new Command.Put(99, seq, "k" + seq, "v"));
        }
        cluster.runFor(200);
        cluster.restart(target);
        assertTrue(cluster.node(target).log().lastIndex() < leader.log().lastIndex());

        cluster.transferLeadership(leader.id(), target);
        assertTrue(cluster.stepUntil(() -> cluster.node(target).role() == RaftNode.Role.LEADER, 140),
                cluster.nodes().toString());
        RaftNode now = cluster.node(target);
        assertTrue(now.log().lastIndex() >= 21, "new leader must hold every write: " + now);
        cluster.runFor(300);
        assertEquals("v", cluster.kv(target).data().get("k20"));
    }

    @Test
    void writesAreRefusedDuringTheTransfer() {
        Cluster cluster = Cluster.builder().nodes(3).seed(3).network(Network.Config.reliable()).clients(2).build();
        cluster.start();
        cluster.startWorkload(4_000);
        RaftNode leader = cluster.awaitLeader(3_000);
        cluster.runFor(500);
        int target = (leader.id() + 1) % 3;
        cluster.crash(target);
        long before = leader.log().lastIndex();
        cluster.transferLeadership(leader.id(), target);
        cluster.runFor(100);
        assertEquals(before, leader.log().lastIndex(), "no new entries while handing over");
        assertEquals(target, leader.transferTarget());

        cluster.runFor(100); // past electionTimeoutMin: the transfer is abandoned
        assertEquals(-1, leader.transferTarget());
        assertEquals(RaftNode.Role.LEADER, leader.role());
        assertTrue(cluster.timeline().render().contains("timed out, still leading"));
        cluster.restart(target);
        assertTrue(cluster.awaitClientsIdle(10_000));
        assertTrue(leader.log().lastIndex() > before, "writes resume after the transfer is abandoned");
        cluster.checker().checkAll();
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
        assertTrue(LinearizabilityChecker.check(cluster.history()).linearizable());
    }

    @Test
    void rejectsBadRequests() {
        Cluster cluster = cluster(1, RaftConfig.defaults());
        RaftNode leader = cluster.awaitLeader(3_000);
        RaftNode follower = cluster.node((leader.id() + 1) % 5);
        assertThrows(IllegalStateException.class, () -> follower.transferLeadership(leader.id()));
        assertThrows(IllegalArgumentException.class, () -> leader.transferLeadership(17));
        leader.transferLeadership(leader.id()); // no-op
        assertEquals(-1, leader.transferTarget());
        assertEquals(RaftNode.Role.LEADER, leader.role());
    }
}
