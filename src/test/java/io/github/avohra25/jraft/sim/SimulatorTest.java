package io.github.avohra25.jraft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SimulatorTest {

    @Test
    void runsEventsInTimeOrderAndFifoForTies() {
        Simulator sim = new Simulator(1);
        List<String> order = new ArrayList<>();
        sim.schedule(30, () -> order.add("c@30"));
        sim.schedule(10, () -> order.add("a@10"));
        sim.schedule(10, () -> order.add("b@10"));
        sim.schedule(0, () -> order.add("now"));
        while (sim.step()) {
            order.add("t=" + sim.now());
        }
        assertEquals(List.of("now", "t=0", "a@10", "t=10", "b@10", "t=10", "c@30", "t=30"), order);
    }

    @Test
    void eventsCanScheduleMoreEvents() {
        Simulator sim = new Simulator(1);
        List<Long> ticks = new ArrayList<>();
        Runnable[] tick = new Runnable[1];
        tick[0] = () -> {
            ticks.add(sim.now());
            if (ticks.size() < 5) {
                sim.schedule(7, tick[0]);
            }
        };
        sim.schedule(7, tick[0]);
        sim.runUntil(1_000);
        assertEquals(List.of(7L, 14L, 21L, 28L, 35L), ticks);
        assertEquals(1_000, sim.now());
    }

    @Test
    void cancelledTimersNeverFire() {
        Simulator sim = new Simulator(1);
        boolean[] fired = new boolean[2];
        Simulator.Timer t = sim.schedule(5, () -> fired[0] = true);
        sim.schedule(6, () -> fired[1] = true);
        t.cancel();
        sim.runFor(10);
        assertFalse(fired[0]);
        assertTrue(fired[1]);
        assertTrue(t.isCancelled());
        assertEquals(1, sim.eventsExecuted());
    }

    @Test
    void runUntilStopsAtTheBoundary() {
        Simulator sim = new Simulator(1);
        List<Long> seen = new ArrayList<>();
        for (long t : new long[] {5, 10, 15}) {
            sim.schedule(t, () -> seen.add(sim.now()));
        }
        sim.runUntil(10);
        assertEquals(List.of(5L, 10L), seen);
        sim.runUntil(20);
        assertEquals(List.of(5L, 10L, 15L), seen);
    }

    @Test
    void sameSeedSameRandomness() {
        Simulator a = new Simulator(42);
        Simulator b = new Simulator(42);
        for (int i = 0; i < 100; i++) {
            assertEquals(a.uniform(0, 1_000), b.uniform(0, 1_000));
        }
    }

    @Test
    void uniformStaysInRange() {
        Simulator sim = new Simulator(3);
        for (int i = 0; i < 10_000; i++) {
            long v = sim.uniform(150, 300);
            assertTrue(v >= 150 && v <= 300, "out of range: " + v);
        }
        assertEquals(4, sim.uniform(4, 4));
    }

    @Test
    void rejectsNegativeDelays() {
        Simulator sim = new Simulator(1);
        assertThrows(IllegalArgumentException.class, () -> sim.schedule(-1, () -> { }));
    }
}
