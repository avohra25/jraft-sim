package io.github.avohra25.jraft.harness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A human-readable record of what happened during a simulation, in simulated-time order. */
public final class Timeline {

    public record Event(long time, String who, String what) {
        @Override
        public String toString() {
            return String.format("%7dms  %-5s %s", time, who, what);
        }
    }

    private final List<Event> events = new ArrayList<>();

    public void add(long time, String who, String what) {
        events.add(new Event(time, who, what));
    }

    public List<Event> events() {
        return Collections.unmodifiableList(events);
    }

    public String render() {
        StringBuilder out = new StringBuilder();
        for (Event event : events) {
            out.append(event).append('\n');
        }
        return out.toString();
    }
}
