package io.github.avohra25.jraft.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

class KvStoreTest {

    @Test
    void basicOperations() {
        KvStore kv = new KvStore();
        assertEquals("OK", kv.apply(new Command.Put(1, 1, "a", "x")));
        assertEquals("OK", kv.apply(new Command.Append(1, 2, "a", "y")));
        assertEquals("OK", kv.apply(new Command.Append(1, 3, "b", "z")));
        assertEquals("xy", kv.apply(new Command.Get(1, 4, "a")));
        assertEquals("OK", kv.apply(new Command.Delete(1, 5, "a")));
        assertNull(kv.apply(new Command.Get(1, 6, "a")));
        assertNull(kv.apply(new Command.Noop()));
        assertEquals(Map.of("b", "z"), kv.data());
    }

    @Test
    void retriesAreAppliedExactlyOnce() {
        KvStore kv = new KvStore();
        kv.apply(new Command.Append(7, 1, "k", "a"));
        kv.apply(new Command.Append(7, 1, "k", "a")); // retry of seq 1
        kv.apply(new Command.Append(8, 1, "k", "b")); // a different client
        assertEquals("ab", kv.get("k"));
        assertEquals("ab", kv.apply(new Command.Get(7, 2, "k")));
        kv.apply(new Command.Append(8, 2, "k", "c"));
        // A retried read returns the value it saw the first time, not the current one.
        assertEquals("ab", kv.apply(new Command.Get(7, 2, "k")));
        // A very late duplicate of an older request is ignored.
        kv.apply(new Command.Append(7, 1, "k", "a"));
        assertEquals("abc", kv.get("k"));
    }

    @Test
    void snapshotRoundTripIncludesSessions() {
        KvStore kv = new KvStore();
        kv.apply(new Command.Put(1, 1, "k", "v\u00e9"));
        kv.apply(new Command.Get(2, 1, "missing"));
        kv.apply(new Command.Append(3, 4, "j", "w"));
        KvStore copy = new KvStore();
        copy.restore(kv.snapshot());
        assertEquals(kv.data(), copy.data());
        copy.apply(new Command.Append(3, 4, "j", "w")); // still deduplicated after restore
        assertEquals("w", copy.get("j"));
        assertNull(copy.apply(new Command.Get(2, 1, "missing")));
    }
}
