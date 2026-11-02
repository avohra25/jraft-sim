package io.github.avohra25.jraft.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.avohra25.jraft.check.LinearizabilityChecker;

/**
 * The scenario random chaos never found: Figure 8 of the Raft paper, replayed step by step. With
 * the real commit rule nothing goes wrong; with the classic bug the checkers must flag it.
 */
class Figure8Test {

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 7, 42})
    void realCommitRuleSurvivesFigure8(long seed) {
        Cluster cluster = Figure8.cluster(seed, false);
        cluster.start();
        Figure8.Outcome outcome = Figure8.drive(cluster);

        assertTrue(outcome.oldEntryOnMajority(), "the script must reach the dangerous state");
        assertEquals(3, outcome.n0CommitIndex(), "n0 commits only once its own term-3 no-op is on a majority");
        assertEquals(3, outcome.committedThroughTerm());
        assertFalse(outcome.staleLeaderElected(), "n4 must not win with its divergent log");
        cluster.checker().checkAll();
        assertTrue(cluster.checker().ok(), cluster.checker().violations().toString());
        assertTrue(cluster.awaitConvergence(3_000), cluster.nodes().toString());
        assertTrue(cluster.leader().isPresent());
        assertEquals("written-in-term-1", cluster.kv(cluster.leader().get().id()).data().get("x"),
                "the term-1 write survives because it was committed the safe way");
        assertTrue(LinearizabilityChecker.check(cluster.history()).linearizable());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 7, 42})
    void unsafeCommitRuleIsCaught(long seed) {
        Cluster cluster = Figure8.cluster(seed, true);
        cluster.start();
        Figure8.Outcome outcome = Figure8.drive(cluster);

        assertTrue(outcome.oldEntryOnMajority());
        assertEquals(2, outcome.n0CommitIndex(), "the bug commits the term-1 entry by counting replicas");
        assertEquals(1, outcome.committedThroughTerm());
        assertTrue(outcome.staleLeaderElected(), "n4 wins and overwrites the 'committed' entry");
        cluster.checker().checkAll();
        assertFalse(cluster.checker().ok());
        String violations = cluster.checker().violations().toString();
        assertTrue(violations.contains("leader completeness"), violations);
        assertTrue(violations.contains("state machine safety") || violations.contains("committed entry changed"),
                violations);
    }
}
