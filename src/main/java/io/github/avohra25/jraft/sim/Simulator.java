package io.github.avohra25.jraft.sim;

import java.util.PriorityQueue;
import java.util.Random;

/**
 * A single-threaded discrete-event simulator.
 *
 * <p>Time is a plain {@code long} counting simulated milliseconds. Events scheduled for the same
 * instant run in the order they were scheduled, and all randomness flows from one seeded
 * {@link Random}, so a run is a pure function of its seed: the same seed always produces the
 * same sequence of events.
 */
public final class Simulator {

    /** Handle to a scheduled event; cancelling it guarantees the action will not run. */
    public interface Timer {
        void cancel();

        boolean isCancelled();

        long deadline();
    }

    private static final class Event implements Comparable<Event>, Timer {
        private final long time;
        private final long seq;
        private final Runnable action;
        private boolean cancelled;

        Event(long time, long seq, Runnable action) {
            this.time = time;
            this.seq = seq;
            this.action = action;
        }

        @Override
        public int compareTo(Event other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(seq, other.seq);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public long deadline() {
            return time;
        }
    }

    private final PriorityQueue<Event> queue = new PriorityQueue<>();
    private final Random random;
    private final long seed;
    private long now;
    private long nextSeq;
    private long executed;

    public Simulator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    public long seed() {
        return seed;
    }

    /** Current simulated time in milliseconds. */
    public long now() {
        return now;
    }

    /** The one source of randomness for everything running inside this simulation. */
    public Random random() {
        return random;
    }

    /** Uniformly random long in {@code [min, max]}. */
    public long uniform(long min, long max) {
        if (max < min) {
            throw new IllegalArgumentException("max < min: " + max + " < " + min);
        }
        return min + random.nextLong(max - min + 1);
    }

    public Timer schedule(long delay, Runnable action) {
        if (delay < 0) {
            throw new IllegalArgumentException("negative delay: " + delay);
        }
        Event event = new Event(now + delay, nextSeq++, action);
        queue.add(event);
        return event;
    }

    /** Runs the next pending event. Returns false when nothing is left to run. */
    public boolean step() {
        Event event;
        while ((event = queue.poll()) != null) {
            if (event.cancelled) {
                continue;
            }
            now = event.time;
            executed++;
            event.action.run();
            return true;
        }
        return false;
    }

    /** Runs every event due at or before {@code time}, then advances the clock to {@code time}. */
    public void runUntil(long time) {
        while (!queue.isEmpty()) {
            Event head = queue.peek();
            if (head.cancelled) {
                queue.poll();
                continue;
            }
            if (head.time > time) {
                break;
            }
            step();
        }
        now = Math.max(now, time);
    }

    public void runFor(long duration) {
        runUntil(now + duration);
    }

    /** Number of events executed so far (cancelled events are not counted). */
    public long eventsExecuted() {
        return executed;
    }

    /** Number of queued events, including cancelled ones not yet discarded. */
    public int pending() {
        return queue.size();
    }
}
