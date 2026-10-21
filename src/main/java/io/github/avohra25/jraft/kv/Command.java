package io.github.avohra25.jraft.kv;

/**
 * A command stored in the replicated log.
 *
 * <p>{@link Noop} is appended by every new leader so it can commit entries left over from earlier
 * terms. Client commands carry a {@code (clientId, seq)} pair that the state machine uses to apply
 * each client operation exactly once, even when the client retries it through several leaders.
 */
public sealed interface Command {

    record Noop() implements Command {}

    sealed interface ClientCommand extends Command {
        int clientId();

        long seq();

        String key();
    }

    record Put(int clientId, long seq, String key, String value) implements ClientCommand {}

    record Append(int clientId, long seq, String key, String value) implements ClientCommand {}

    record Delete(int clientId, long seq, String key) implements ClientCommand {}

    record Get(int clientId, long seq, String key) implements ClientCommand {}
}
