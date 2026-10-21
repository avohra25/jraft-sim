package io.github.avohra25.jraft.harness;

import java.util.List;

import io.github.avohra25.jraft.check.History;
import io.github.avohra25.jraft.kv.Command;
import io.github.avohra25.jraft.raft.Message;
import io.github.avohra25.jraft.sim.Network;
import io.github.avohra25.jraft.sim.Simulator;

/**
 * A closed-loop client: it has at most one operation outstanding and retries it, with the same
 * sequence number, until some leader answers. Requests go to the node it believes is the leader;
 * rejections carry a leader hint, and timeouts make it try another node.
 */
public final class KvClient {

    private static final long REQUEST_TIMEOUT = 200;

    private final int id;
    private final Simulator sim;
    private final Network<Message> network;
    private final List<Integer> servers;
    private final List<String> keys;
    private final History history;
    private final Timeline timeline;

    private long seq;
    private Command.ClientCommand current;
    private History.Op op;
    private int target;
    private Simulator.Timer timer;
    private long stopAt;
    private long completed;

    KvClient(int id, Simulator sim, Network<Message> network, List<Integer> servers, List<String> keys,
             History history, Timeline timeline) {
        this.id = id;
        this.sim = sim;
        this.network = network;
        this.servers = List.copyOf(servers);
        this.keys = List.copyOf(keys);
        this.history = history;
        this.timeline = timeline;
        this.target = servers.get(sim.random().nextInt(servers.size()));
        network.register(id, envelope -> onMessage(envelope.message()));
    }

    public int id() {
        return id;
    }

    /** Starts issuing random operations until simulated time {@code stopAt}. */
    void start(long stopAt) {
        this.stopAt = stopAt;
        rearm(sim.uniform(1, 20), this::issueNext);
    }

    /** True while an operation is outstanding. */
    public boolean busy() {
        return current != null;
    }

    public long completed() {
        return completed;
    }

    private void issueNext() {
        if (sim.now() >= stopAt) {
            return;
        }
        seq++;
        String key = keys.get(sim.random().nextInt(keys.size()));
        String value = "c" + id + "." + seq + ";";
        int roll = sim.random().nextInt(100);
        History.Kind kind;
        if (roll < 40) {
            kind = History.Kind.GET;
            current = new Command.Get(id, seq, key);
        } else if (roll < 65) {
            kind = History.Kind.PUT;
            current = new Command.Put(id, seq, key, value);
        } else if (roll < 95) {
            kind = History.Kind.APPEND;
            current = new Command.Append(id, seq, key, value);
        } else {
            kind = History.Kind.DELETE;
            current = new Command.Delete(id, seq, key);
        }
        op = history.invoke(id, seq, kind, key, value, sim.now());
        sendCurrent();
    }

    private void sendCurrent() {
        network.send(id, target, new Message.ClientRequest(current));
        rearm(REQUEST_TIMEOUT, () -> {
            target = otherServer();
            sendCurrent();
        });
    }

    private void onMessage(Message message) {
        if (!(message instanceof Message.ClientReply reply) || current == null || reply.seq() != seq) {
            return; // stale or duplicate reply
        }
        if (reply.ok()) {
            history.complete(op, sim.now(), reply.result());
            if (timeline != null) {
                timeline.add(sim.now(), "c" + id, op.kind() + " " + op.key()
                        + (op.kind() == History.Kind.GET ? " -> " + reply.result() : "")
                        + " via n" + reply.leaderHint() + " (" + (sim.now() - op.invokedAt()) + "ms)");
            }
            current = null;
            op = null;
            completed++;
            target = reply.leaderHint();
            rearm(sim.uniform(1, 30), this::issueNext);
        } else {
            target = reply.leaderHint() >= 0 ? reply.leaderHint() : otherServer();
            rearm(sim.uniform(5, 25), this::sendCurrent);
        }
    }

    private int otherServer() {
        if (servers.size() == 1) {
            return servers.getFirst();
        }
        int next;
        do {
            next = servers.get(sim.random().nextInt(servers.size()));
        } while (next == target);
        return next;
    }

    private void rearm(long delay, Runnable action) {
        if (timer != null) {
            timer.cancel();
        }
        timer = sim.schedule(delay, action);
    }
}
