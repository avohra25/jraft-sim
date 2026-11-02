package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.avohra25.jraft.check.LinearizabilityChecker;
import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.sim.Network;

class PreVoteCheckQuorumTest {

    private static Cluster cluster(int nodes, long seed, RaftConfig raft, int clients) {
        Cluster cluster = Cluster.builder().nodes(nodes).seed(seed).network(Network.Config.reliable())
                .raft(raft).clients(clients).build();
        cluster.start();
        return cluster;
    }

    private static int someFollower(Cluster cluster, RaftNode leader) {
        return (leader.id() + 1) % cluster.nodes().size();
    }

    /** Cuts a follower off for a while, then lets it back in. Returns the leader before the cut. */
    private static RaftNode isolateAndRejoin(Cluster cluster, int follower) {
        RaftNode leader = cluster.leader().orElseThrow();
        cluster.isolate(follower);
        cluster.runFor(3_000);
        cluster.heal();
        cluster.runFor(2_000);
        return leader;
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void withoutPreVoteARejoiningNodeDeposesTheLeader(long seed) {
        Cluster cluster = cluster(5, seed, RaftConfig.defaults(), 0);
        RaftNode leader = cluster.awaitLeader(3_000);
        long term = leader.currentTerm();
        int follower = someFollower(cluster, leader);
        isolateAndRejoin(cluster, follower);

        // The cut-off node kept timing out and bumping its term; on its return that term
        // forces everyone, the leader included, to move past it. That is the disruption pre-vote fixes.
        RaftNode now = cluster.awaitLeader(3_000);
        assertTrue(now.currentTerm() > term + 5, "term only went from " + term + " to " + now.currentTerm());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void withPreVoteARejoiningNodeDisruptsNothing(long seed) {
        Cluster cluster = cluster(5, seed, RaftConfig.defaults().withPreVote(true), 0);
        RaftNode leader = cluster.awaitLeader(3_000);
        long term = leader.currentTerm();
        int follower = someFollower(cluster, leader);
        isolateAndRejoin(cluster, follower);

        assertEquals(leader, cluster.leader().orElseThrow(), "the healthy leader must keep its job");
        assertEquals(term, leader.currentTerm());
        RaftNode returned = cluster.node(follower);
        assertEquals(term, returned.currentTerm(), "pre-votes that fail must not raise the term");
        assertEquals(RaftNode.Role.FOLLOWER, returned.role());
        assertEquals(leader.id(), returned.leaderId());
        assertTrue(cluster.timeline().render().contains("asking for pre-votes"), "the cut-off node never polled");
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void preVoteStillReplacesACrashedLeader(long seed) {
        Cluster cluster = cluster(5, seed, RaftConfig.defaults().withPreVote(true).withCheckQuorum(true), 0);
        RaftNode first = cluster.awaitLeader(3_000);
        cluster.crash(first.id());
        RaftNode second = cluster.awaitLeader(3_000);
        assertNotEquals(first.id(), second.id());
        assertTrue(second.currentTerm() > first.currentTerm());
    }

    @Test
    void singleNodeWithPreVoteElectsItself() {
        Cluster cluster = cluster(1, 1, RaftConfig.defaults().withPreVote(true).withCheckQuorum(true), 0);
        RaftNode leader = cluster.awaitLeader(1_000);
        assertEquals(1, leader.currentTerm());
        cluster.runFor(2_000);
        assertEquals(RaftNode.Role.LEADER, leader.role(), "a lone node is always its own quorum");
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void withoutCheckQuorumACutOffLeaderHangsOn(long seed) {
        Cluster cluster = cluster(5, seed, RaftConfig.defaults(), 0);
        RaftNode leader = cluster.awaitLeader(3_000);
        cluster.isolate(leader.id());
        cluster.runFor(2_000);
        assertEquals(RaftNode.Role.LEADER, leader.role());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void withCheckQuorumAMinorityLeaderStepsDown(long seed) {
        Cluster cluster = cluster(5, seed, RaftConfig.defaults().withCheckQuorum(true), 0);
        RaftNode leader = cluster.awaitLeader(3_000);
        int partner = someFollower(cluster, leader);
        List<Integer> majority = cluster.nodes().stream().map(RaftNode::id)
                .filter(id -> id != leader.id() && id != partner).toList();
        cluster.partition(List.of(List.of(leader.id(), partner), majority));

        // One check-quorum window to notice, plus at most one more to have started counting.
        cluster.runFor(2 * RaftConfig.defaults().electionTimeoutMin() + 1);
        assertNotEquals(RaftNode.Role.LEADER, leader.role(), "a leader that can reach 2 of 5 must give up");
        assertEquals(-1, leader.leaderId());
        assertTrue(cluster.timeline().render().contains("giving up leadership (check-quorum)"));

        RaftNode replacement = cluster.awaitLeader(3_000);
        assertTrue(majority.contains(replacement.id()));
        cluster.heal();
        cluster.runFor(1_000);
        assertEquals(replacement, cluster.leader().orElseThrow());
        assertEquals(replacement.id(), leader.leaderId());
    }

    @Test
    void checkQuorumLeaderKeepsItsJobWhileAMajorityAnswers() {
        Cluster cluster = cluster(5, 9, RaftConfig.defaults().withCheckQuorum(true), 0);
        RaftNode leader = cluster.awaitLeader(3_000);
        long term = leader.currentTerm();
        // Two followers gone still leaves 3 of 5.
        cluster.crash(someFollower(cluster, leader));
        cluster.crash((leader.id() + 2) % 5);
        cluster.runFor(5_000);
        assertEquals(leader, cluster.leader().orElseThrow());
        assertEquals(term, leader.currentTerm());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {11, 12, 13})
    void clientsStayCorrectWithBothEnabled(long seed) {
        Cluster cluster = Cluster.builder().nodes(5).seed(seed).network(Network.Config.lossy())
                .raft(RaftConfig.defaults().withPreVote(true).withCheckQuorum(true)).clients(3).build();
        cluster.start();
        cluster.startWorkload(3_000);
        cluster.awaitLeader(3_000);
        RaftNode leader = cluster.leader().orElseThrow();
        cluster.isolate(leader.id());
        cluster.runFor(1_500);
        assertFalse(leader.role() == RaftNode.Role.LEADER, "cut-off leader should have stepped down");
        cluster.heal();
        assertTrue(cluster.awaitClientsIdle(15_000), cluster.nodes().toString());
        cluster.checker().checkAll();
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
        assertTrue(LinearizabilityChecker.check(cluster.history()).linearizable());
        assertTrue(cluster.awaitConvergence(3_000));
    }
}
