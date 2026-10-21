package io.github.avohra25.jraft.harness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import io.github.avohra25.jraft.raft.RaftNode;

/**
 * Random fault injector, named after Jepsen's. Until its deadline it keeps crashing and restarting
 * nodes, partitioning the network and isolating leaders; at the deadline it heals the network and
 * restarts everything so the test can check that the cluster recovers.
 *
 * <p>It deliberately does not keep a majority alive: safety must hold under any fault pattern,
 * only liveness needs the final recovery.
 */
public final class Nemesis {

    private final Cluster cluster;
    private final long minGap;
    private final long maxGap;
    private long until;
    private int faults;

    public Nemesis(Cluster cluster, long minGap, long maxGap) {
        this.cluster = cluster;
        this.minGap = minGap;
        this.maxGap = maxGap;
    }

    public void start(long until) {
        this.until = until;
        scheduleNext();
    }

    public int faultsInjected() {
        return faults;
    }

    private void scheduleNext() {
        long gap = cluster.sim().uniform(minGap, maxGap);
        long at = cluster.sim().now() + gap;
        if (at >= until) {
            cluster.sim().schedule(Math.max(0, until - cluster.sim().now()), this::recover);
        } else {
            cluster.sim().schedule(gap, () -> {
                inject();
                scheduleNext();
            });
        }
    }

    private void inject() {
        Random rng = cluster.sim().random();
        List<RaftNode> nodes = cluster.nodes();
        List<RaftNode> up = nodes.stream().filter(RaftNode::isUp).toList();
        List<RaftNode> down = nodes.stream().filter(n -> !n.isUp()).toList();
        faults++;
        switch (rng.nextInt(7)) {
            case 0, 1 -> {
                if (!up.isEmpty()) {
                    cluster.crash(up.get(rng.nextInt(up.size())).id());
                }
            }
            case 2 -> cluster.leader().ifPresent(l -> cluster.crash(l.id()));
            case 3 -> {
                if (!down.isEmpty()) {
                    cluster.restart(down.get(rng.nextInt(down.size())).id());
                }
            }
            case 4 -> randomPartition(rng, nodes.size());
            case 5 -> cluster.leader().ifPresent(l -> cluster.isolate(l.id()));
            default -> {
                cluster.heal();
                down.forEach(n -> cluster.restart(n.id()));
            }
        }
    }

    private void randomPartition(Random rng, int n) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(i);
        }
        Collections.shuffle(ids, rng);
        int cut = 1 + rng.nextInt(Math.max(1, n - 1));
        cluster.partition(List.of(List.copyOf(ids.subList(0, cut)), List.copyOf(ids.subList(cut, n))));
    }

    private void recover() {
        cluster.heal();
        for (RaftNode node : cluster.nodes()) {
            cluster.restart(node.id());
        }
    }
}
