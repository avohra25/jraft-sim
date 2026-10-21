package io.github.avohra25.jraft.sim;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * An unreliable, asynchronous network running on a {@link Simulator}.
 *
 * <p>Every message gets an independent random delay in {@code [minDelay, maxDelay]}, so messages
 * between the same pair of endpoints are routinely reordered. Messages may also be dropped or
 * duplicated. Links can be cut to model partitions; a cut link drops messages both at send time
 * and at delivery time, so a partition also swallows messages that were already in flight.
 *
 * @param <M> message type
 */
public final class Network<M> {

    public record Config(long minDelay, long maxDelay, double dropRate, double duplicateRate) {
        public Config {
            if (minDelay < 0 || maxDelay < minDelay) {
                throw new IllegalArgumentException("bad delay range [" + minDelay + ", " + maxDelay + "]");
            }
            if (dropRate < 0 || dropRate >= 1 || duplicateRate < 0 || duplicateRate >= 1) {
                throw new IllegalArgumentException("rates must be in [0, 1)");
            }
        }

        /** In-order-ish, lossless delivery with a small fixed delay. */
        public static Config reliable() {
            return new Config(1, 1, 0, 0);
        }

        /** The default lossy profile: 1-15ms delay, 5% drops, 2% duplicates. */
        public static Config lossy() {
            return new Config(1, 15, 0.05, 0.02);
        }
    }

    public record Envelope<M>(int from, int to, M message, long sentAt) {}

    private record Link(int from, int to) {}

    private final Simulator sim;
    private final Map<Integer, Consumer<Envelope<M>>> endpoints = new TreeMap<>();
    private final Set<Link> cut = new HashSet<>();
    private final Map<String, Long> stats = new HashMap<>();
    private Config config;

    public Network(Simulator sim, Config config) {
        this.sim = sim;
        this.config = config;
    }

    public Config config() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public void register(int id, Consumer<Envelope<M>> handler) {
        if (endpoints.putIfAbsent(id, handler) != null) {
            throw new IllegalArgumentException("endpoint " + id + " already registered");
        }
    }

    public void send(int from, int to, M message) {
        count("sent");
        if (!connected(from, to) || sim.random().nextDouble() < config.dropRate()) {
            count("dropped");
            return;
        }
        int copies = sim.random().nextDouble() < config.duplicateRate() ? 2 : 1;
        Envelope<M> envelope = new Envelope<>(from, to, message, sim.now());
        for (int i = 0; i < copies; i++) {
            long delay = sim.uniform(config.minDelay(), config.maxDelay());
            sim.schedule(delay, () -> deliver(envelope));
        }
    }

    private void deliver(Envelope<M> envelope) {
        Consumer<Envelope<M>> handler = endpoints.get(envelope.to());
        if (handler == null || !connected(envelope.from(), envelope.to())) {
            count("dropped");
            return;
        }
        count("delivered");
        handler.accept(envelope);
    }

    public boolean connected(int from, int to) {
        return !cut.contains(new Link(from, to));
    }

    /** Cuts the link in both directions. */
    public void disconnect(int a, int b) {
        cut.add(new Link(a, b));
        cut.add(new Link(b, a));
    }

    /** Cuts only the {@code from -> to} direction, modelling an asymmetric fault. */
    public void disconnectOneWay(int from, int to) {
        cut.add(new Link(from, to));
    }

    /** Cuts every link to and from {@code id}, including links to endpoints registered later. */
    public void isolate(int id) {
        for (int other : endpoints.keySet()) {
            if (other != id) {
                disconnect(id, other);
            }
        }
    }

    /**
     * Replaces the current partition with one where endpoints in different groups cannot talk.
     * Endpoints not named in any group (for example clients) keep talking to everyone.
     */
    public void partition(List<? extends Collection<Integer>> groups) {
        heal();
        for (int g = 0; g < groups.size(); g++) {
            for (int h = g + 1; h < groups.size(); h++) {
                for (int a : groups.get(g)) {
                    for (int b : groups.get(h)) {
                        disconnect(a, b);
                    }
                }
            }
        }
    }

    public void heal() {
        cut.clear();
    }

    public boolean isPartitioned() {
        return !cut.isEmpty();
    }

    public long stat(String name) {
        return stats.getOrDefault(name, 0L);
    }

    private void count(String name) {
        stats.merge(name, 1L, Long::sum);
    }
}
