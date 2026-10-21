# jraft-sim

Raft consensus in Java 21, running on a deterministic discrete-event network simulator.

Work in progress. The simulator core is here; Raft itself, the replicated key-value store,
the invariant checkers and a CLI are coming next.

## Build and test

Requires JDK 21 and Maven 3.9+.

```sh
mvn test
```

## License

MIT, see [LICENSE](LICENSE).
