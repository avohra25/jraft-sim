package io.github.avohra25.jraft.harness;

import io.github.avohra25.jraft.kv.Command;
import io.github.avohra25.jraft.raft.RaftConfig;
import io.github.avohra25.jraft.raft.RaftNode;
import io.github.avohra25.jraft.sim.Network;

/**
 * A scripted replay of Figure 8 from the Raft paper on five nodes (paper S1..S5 are n0..n4).
 *
 * <p>The figure shows why a leader must not commit an entry from an earlier term just because it
 * sits on a majority of servers:
 *
 * <ol>
 *   <li>n0 leads term 1 and replicates entry 2 (a term-1 write) to n1 only.</li>
 *   <li>n0 crashes. n4 wins the next term with votes from n2 and n3, appends its no-op at index 2
 *       and crashes before replicating it.</li>
 *   <li>n0 restarts and wins a later term. It copies its old term-1 entry 2 to n2, so that entry
 *       now sits on n0, n1 and n2: a majority. Then n0 crashes.</li>
 *   <li>n4 restarts and campaigns. Its last log term (from step 2) is newer than the term-1
 *       entry, so n2 and n3 vote for it, and as leader it overwrites index 2 everywhere.</li>
 * </ol>
 *
 * <p>If n0 had marked entry 2 committed in step 3 (the {@link RaftConfig#unsafeCommitRule()}
 * mutation), step 4 destroys a committed entry and the invariant checker reports leader
 * completeness and state machine safety violations. With the real rule n0 only commits through
 * an entry of its own term; by the time it may do that, the entry is on a majority whose logs end
 * in n0's term, so n4 can no longer collect enough votes.
 *
 * <p>The script needs exact control over who campaigns and when, so it runs with very long
 * election timeouts (nobody campaigns unless told to), one entry per AppendEntries (so entries
 * travel one round trip at a time, as in the figure) and a lossless network.
 */
public final class Figure8 {

    /**
     * What the run showed.
     *
     * @param oldEntryOnMajority  the term-1 entry at index 2 was on a majority when n0 stopped
     * @param committedThroughTerm term of the entry n0 had committed through when it crashed
     * @param n0CommitIndex       n0's commit index when it crashed
     * @param staleLeaderElected  n4 won an election with its divergent index 2
     */
    public record Outcome(boolean oldEntryOnMajority, long committedThroughTerm, long n0CommitIndex,
                          boolean staleLeaderElected) {}

    public static final int NODES = 5;
    private static final long OLD_INDEX = 2;

    private Figure8() {}

    /** The Raft settings the script relies on, on top of {@code base} (snapshots stay off). */
    public static RaftConfig config(RaftConfig base) {
        return base.withElectionTimeout(10_000, 20_000).withMaxEntriesPerAppend(1).withSnapshotThreshold(0);
    }

    public static Network.Config network() {
        return Network.Config.reliable();
    }

    public static Cluster cluster(long seed, boolean unsafeCommitRule) {
        return Cluster.builder()
                .nodes(NODES)
                .seed(seed)
                .network(network())
                .raft(config(RaftConfig.defaults().withUnsafeCommitRule(unsafeCommitRule)))
                .clients(0)
                .build();
    }

    /** Runs the script on a started five-node cluster built with {@link #config} and {@link #network}. */
    public static Outcome drive(Cluster c) {
        if (c.nodes().size() != NODES) {
            throw new IllegalArgumentException("the Figure 8 script needs exactly " + NODES + " nodes");
        }
        RaftNode n0 = c.node(0);
        RaftNode n4 = c.node(4);

        // (a) n0 leads term 1; its no-op (index 1) reaches everyone. Then a term-1 write reaches n1 only.
        elect(c, 0);
        c.runFor(200);
        for (int peer = 2; peer <= 4; peer++) {
            c.disconnect(0, peer);
        }
        c.propose(0, new Command.Put(Cluster.SCRIPT_CLIENT, 1, "x", "written-in-term-1"));
        c.runFor(50);

        // (b) n0 crashes; n4 wins with n2 and n3, appends its no-op at index 2, crashes at once.
        c.crash(0);
        c.heal();
        elect(c, 4);
        c.isolate(4); // its first AppendEntries are already in flight; make sure they are lost
        c.crash(4);
        c.runFor(20);
        c.heal();

        // (c) n0 comes back, wins a later term and pushes entry 2 to the others one round trip at a
        //     time. Stop it the moment it advances its commit index past the old entry.
        c.restart(0);
        elect(c, 0);
        c.stepUntil(() -> n0.commitIndex() >= OLD_INDEX, 1_000);
        long n0Commit = n0.commitIndex();
        long committedTerm = n0.log().termAt(n0Commit);
        long oldTerm = n0.log().termAt(OLD_INDEX);
        int holders = 0;
        for (RaftNode node : c.nodes()) {
            if (node.log().lastIndex() >= OLD_INDEX && node.log().termAt(OLD_INDEX) == oldTerm) {
                holders++;
            }
        }
        c.isolate(0); // nothing n0 sent at that instant gets out
        c.crash(0);
        c.runFor(20);
        c.heal();

        // (d) n4 comes back and campaigns with its divergent index 2.
        c.restart(4);
        boolean stale = tryElect(c, 4) && n4.log().termAt(OLD_INDEX) != oldTerm;

        // Recovery: bring n0 back and make sure the cluster ends with a leader.
        c.restart(0);
        if (c.leader().isEmpty()) {
            for (int id : new int[] {1, 0, 2, 3}) {
                if (tryElect(c, id)) {
                    break;
                }
            }
        }
        c.runFor(300);
        return new Outcome(holders > NODES / 2, committedTerm, n0Commit, stale);
    }

    private static void elect(Cluster c, int id) {
        if (!tryElect(c, id)) {
            throw new IllegalStateException("n" + id + " could not get elected: " + c.nodes());
        }
    }

    /** Campaigns a few times (each failed attempt still teaches the voters a newer term). */
    private static boolean tryElect(Cluster c, int id) {
        RaftNode node = c.node(id);
        for (int attempt = 0; attempt < 4; attempt++) {
            c.campaign(id);
            if (c.stepUntil(() -> node.role() == RaftNode.Role.LEADER, 100)) {
                return true;
            }
        }
        return false;
    }
}
