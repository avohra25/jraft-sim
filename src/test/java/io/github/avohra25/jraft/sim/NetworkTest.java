package io.github.avohra25.jraft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class NetworkTest {

    private static List<Network.Envelope<String>> inbox(Network<String> net, int id) {
        List<Network.Envelope<String>> received = new ArrayList<>();
        net.register(id, received::add);
        return received;
    }

    @Test
    void deliversWithinTheDelayBounds() {
        Simulator sim = new Simulator(7);
        Network<String> net = new Network<>(sim, new Network.Config(5, 20, 0, 0));
        inbox(net, 1);
        List<Network.Envelope<String>> got = inbox(net, 2);
        List<Long> arrivals = new ArrayList<>();
        net.register(3, e -> arrivals.add(sim.now() - e.sentAt()));
        for (int i = 0; i < 500; i++) {
            net.send(1, 3, "m" + i);
        }
        net.send(1, 2, "hello");
        sim.runFor(100);
        assertEquals(1, got.size());
        assertEquals("hello", got.getFirst().message());
        assertEquals(1, got.getFirst().from());
        assertEquals(500, arrivals.size());
        assertTrue(arrivals.stream().allMatch(d -> d >= 5 && d <= 20));
    }

    @Test
    void randomDelaysReorderMessages() {
        Simulator sim = new Simulator(11);
        Network<String> net = new Network<>(sim, new Network.Config(1, 50, 0, 0));
        List<Network.Envelope<String>> got = inbox(net, 2);
        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            sent.add("m" + i);
            net.send(1, 2, "m" + i);
        }
        sim.runFor(100);
        List<String> order = got.stream().map(Network.Envelope::message).toList();
        assertEquals(Set.copyOf(sent), Set.copyOf(order));
        assertNotEquals(sent, order);
    }

    @Test
    void dropsAndDuplicatesAtRoughlyTheConfiguredRates() {
        Simulator sim = new Simulator(5);
        Network<String> net = new Network<>(sim, new Network.Config(1, 3, 0.2, 0.1));
        List<Network.Envelope<String>> got = inbox(net, 2);
        int n = 20_000;
        for (int i = 0; i < n; i++) {
            net.send(1, 2, "x");
        }
        sim.runFor(10);
        double expected = n * 0.8 * 1.1;
        assertEquals(expected, got.size(), expected * 0.05);
        assertEquals(n, net.stat("sent"));
    }

    @Test
    void partitionsBlockTrafficBetweenGroupsOnly() {
        Simulator sim = new Simulator(1);
        Network<String> net = new Network<>(sim, Network.Config.reliable());
        List<List<Network.Envelope<String>>> boxes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            boxes.add(inbox(net, i));
        }
        net.partition(List.of(List.of(0, 1), List.of(2, 3)));
        net.send(0, 1, "same side");
        net.send(0, 2, "across");
        net.send(3, 1, "across");
        net.send(2, 3, "same side");
        sim.runFor(5);
        assertEquals(1, boxes.get(1).size());
        assertEquals(0, boxes.get(2).size());
        assertEquals(1, boxes.get(3).size());
        net.heal();
        net.send(0, 2, "healed");
        sim.runFor(5);
        assertEquals(1, boxes.get(2).size());
    }

    @Test
    void cuttingALinkAlsoDropsMessagesInFlight() {
        Simulator sim = new Simulator(1);
        Network<String> net = new Network<>(sim, new Network.Config(10, 10, 0, 0));
        List<Network.Envelope<String>> got = inbox(net, 2);
        inbox(net, 1);
        net.send(1, 2, "doomed");
        sim.runFor(5);
        net.isolate(2);
        sim.runFor(10);
        assertTrue(got.isEmpty());
        assertEquals(1, net.stat("dropped"));
    }

    @Test
    void oneWayLinks() {
        Simulator sim = new Simulator(1);
        Network<String> net = new Network<>(sim, Network.Config.reliable());
        List<Network.Envelope<String>> one = inbox(net, 1);
        List<Network.Envelope<String>> two = inbox(net, 2);
        net.disconnectOneWay(1, 2);
        net.send(1, 2, "lost");
        net.send(2, 1, "arrives");
        sim.runFor(5);
        assertTrue(two.isEmpty());
        assertEquals(1, one.size());
    }

    @Test
    void validatesConfigAndRegistration() {
        assertThrows(IllegalArgumentException.class, () -> new Network.Config(5, 1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new Network.Config(1, 2, 1.0, 0));
        Network<String> net = new Network<>(new Simulator(1), Network.Config.reliable());
        net.register(1, e -> { });
        assertThrows(IllegalArgumentException.class, () -> net.register(1, e -> { }));
    }
}
