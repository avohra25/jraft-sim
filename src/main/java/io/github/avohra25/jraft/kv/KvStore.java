package io.github.avohra25.jraft.kv;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import io.github.avohra25.jraft.raft.StateMachine;

/**
 * The replicated key-value state machine.
 *
 * <p>Besides the data itself it keeps one session per client holding the last sequence number it
 * applied and that operation's result. A command whose sequence number was already applied is not
 * executed again; the cached result is returned instead, which makes client retries safe.
 */
public final class KvStore implements StateMachine {

    private record Session(long seq, String result) {}

    private final TreeMap<String, String> data = new TreeMap<>();
    private final Map<Integer, Session> sessions = new HashMap<>();
    private long applied;

    @Override
    public String apply(Command command) {
        applied++;
        return switch (command) {
            case Command.Noop noop -> null;
            case Command.ClientCommand cmd -> applyOnce(cmd);
        };
    }

    private String applyOnce(Command.ClientCommand cmd) {
        Session session = sessions.get(cmd.clientId());
        if (session != null && cmd.seq() <= session.seq()) {
            // A retry of an operation we already executed: answer it without re-executing.
            return cmd.seq() == session.seq() ? session.result() : null;
        }
        String result = execute(cmd);
        sessions.put(cmd.clientId(), new Session(cmd.seq(), result));
        return result;
    }

    private String execute(Command.ClientCommand cmd) {
        return switch (cmd) {
            case Command.Put put -> {
                data.put(put.key(), put.value());
                yield "OK";
            }
            case Command.Append append -> {
                data.merge(append.key(), append.value(), String::concat);
                yield "OK";
            }
            case Command.Delete delete -> {
                data.remove(delete.key());
                yield "OK";
            }
            case Command.Get get -> data.get(get.key());
        };
    }

    public String get(String key) {
        return data.get(key);
    }

    public SortedMap<String, String> data() {
        return Collections.unmodifiableSortedMap(data);
    }

    /** Number of log entries applied to this instance (including no-ops and duplicates). */
    public long appliedCount() {
        return applied;
    }
}
