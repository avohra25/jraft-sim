package io.github.avohra25.jraft.raft;

import io.github.avohra25.jraft.kv.Command;

/** The deterministic state machine that committed log entries are applied to, in index order. */
public interface StateMachine {

    /** Applies one committed command and returns its result (may be null). */
    String apply(Command command);
}
