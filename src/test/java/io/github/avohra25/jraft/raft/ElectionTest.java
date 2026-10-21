package io.github.avohra25.jraft.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.sim.Network;

class ElectionTest {

    private static Cluster cluster(int nodes, long seed) {
        Cluster cluster = Cluster.builder().nodes(nodes).seed(seed).clients(0).build();
        cluster.start();
        return cluster;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void electsOneLeaderThatEveryoneFollows(long seed) {
        Cluster cluster = cluster(5, seed);
        RaftNode leader = cluster.awaitLeader(3_000);
        cluster.runFor(1_000);
        assertEquals(leader, cluster.leader().orElseThrow());
        for (RaftNode node : cluster.nodes()) {
            assertEquals(leader.currentTerm(), node.currentTerm(), node.toString());
            assertEquals(leader.id(), node.leaderId(), node.toString());
            if (node != leader) {
                assertEquals(RaftNode.Role.FOLLOWER, node.role());
            }
        }
    }

    @Test
    void leaderIsStableOnAReliableNetwork() {
        Cluster cluster = Cluster.builder().nodes(5).seed(3).clients(0).network(Network.Config.reliable()).build();
        cluster.start();
        RaftNode leader = cluster.awaitLeader(3_000);
        long term = leader.currentTerm();
        cluster.runFor(20_000);
        assertEquals(leader, cluster.leader().orElseThrow());
        assertEquals(term, leader.currentTerm());
    }

    @Test
    void crashedLeaderIsReplacedInAHigherTerm() {
        Cluster cluster = cluster(5, 11);
        RaftNode first = cluster.awaitLeader(3_000);
        cluster.crash(first.id());
        RaftNode second = cluster.awaitLeader(3_000);
        assertNotEquals(first.id(), second.id());
        assertTrue(second.currentTerm() > first.currentTerm());

        cluster.restart(first.id());
        cluster.runFor(1_000);
        assertEquals(RaftNode.Role.FOLLOWER, first.role());
        assertEquals(second.currentTerm(), first.currentTerm());
    }

    @Test
    void noLeaderWithoutAMajority() {
        Cluster cluster = cluster(5, 5);
        cluster.awaitLeader(3_000);
        for (int id : List.of(0, 1, 2)) {
            cluster.crash(id);
        }
        cluster.runFor(5_000);
        assertTrue(cluster.leader().isEmpty(), "two of five nodes elected a leader");
        cluster.restart(1);
        cluster.awaitLeader(3_000);
    }

    @Test
    void isolatedLeaderIsReplacedAndStepsDownAfterHealing() {
        Cluster cluster = cluster(5, 8);
        RaftNode old = cluster.awaitLeader(3_000);
        cluster.isolate(old.id());
        cluster.runFor(2_000);
        RaftNode fresh = cluster.leader().orElseThrow();
        assertNotEquals(old.id(), fresh.id());
        assertEquals(RaftNode.Role.LEADER, old.role(), "an isolated leader cannot know it was deposed");
        cluster.heal();
        cluster.runFor(1_000);
        assertEquals(RaftNode.Role.FOLLOWER, old.role());
    }

    @Test
    void termAndVoteSurviveACrash() {
        Cluster cluster = cluster(3, 2);
        cluster.awaitLeader(3_000);
        cluster.runFor(500);
        RaftNode node = cluster.node(0);
        long term = node.currentTerm();
        int vote = node.votedFor();
        cluster.crash(0);
        cluster.restart(0);
        assertEquals(term, node.currentTerm());
        assertEquals(vote, node.votedFor());
    }

    @Test
    void singleNodeClusterElectsItself() {
        Cluster cluster = cluster(1, 1);
        RaftNode leader = cluster.awaitLeader(1_000);
        assertEquals(0, leader.id());
        assertEquals(1, leader.commitIndex(), "the leader's no-op commits immediately");
    }
}
