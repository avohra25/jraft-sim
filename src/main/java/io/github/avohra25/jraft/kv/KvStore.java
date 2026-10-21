package io.github.avohra25.jraft.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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

    @Override
    public byte[] snapshot() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                writeString(out, e.getKey());
                writeString(out, e.getValue());
            }
            // Sessions are part of the state: dropping them would let a retry execute twice.
            TreeMap<Integer, Session> ordered = new TreeMap<>(sessions);
            out.writeInt(ordered.size());
            for (Map.Entry<Integer, Session> e : ordered.entrySet()) {
                out.writeInt(e.getKey());
                out.writeLong(e.getValue().seq());
                writeString(out, e.getValue().result());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    @Override
    public void restore(byte[] snapshot) {
        data.clear();
        sessions.clear();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            int entries = in.readInt();
            for (int i = 0; i < entries; i++) {
                data.put(readString(in), readString(in));
            }
            int clients = in.readInt();
            for (int i = 0; i < clients; i++) {
                sessions.put(in.readInt(), new Session(in.readLong(), readString(in)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt snapshot", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(utf8.length);
        out.write(utf8);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        return new String(in.readNBytes(length), StandardCharsets.UTF_8);
    }
}
