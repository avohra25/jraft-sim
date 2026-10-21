# jraft-sim

Raft consensus in Java 21 (no runtime dependencies), running on a **deterministic discrete-event
network simulator**. The simulator delays, drops, duplicates and reorders messages, partitions
the network and crashes and restarts nodes. Everything comes from one seeded RNG, so a seed fully
describes a run and any failure replays exactly.

On top of Raft sits a replicated key-value store with exactly-once client sessions. Every
randomized run is checked two ways: **online invariant checks** of Raft's safety properties, and
a **linearizability check** of what the clients actually saw.

```
$ java -jar target/jraft-sim-0.1.0.jar --scenario partition --seed 4 --duration 3000
      0ms  sim   cluster of 5 nodes started (seed 4)
    168ms  n3    election timeout -> CANDIDATE for term 1
    182ms  n3    became LEADER for term 1 (log last index 0)
    585ms  net   PARTITION {n3,n0} | {n1,n2,n4}
    720ms  n4    election timeout -> CANDIDATE for term 2
    739ms  n4    became LEADER for term 2 (log last index 21)
   1585ms  net   HEALED
   1594ms  n3    LEADER -> FOLLOWER, term 2

final state
  n0  FOLLOWER  term 2   commit 95    log 1..95  kv {k0=c102.29;, k1=c101.29;, k2=c101.27;c100.24;}
  ...
  n4  LEADER    term 2   commit 95    log 1..95  kv {k0=c102.29;, k1=c101.29;, k2=c101.27;c100.24;}
network: 2164 sent, 1921 delivered, 259 dropped; 2208 events simulated
clients: 85 operations completed, 0 pending
invariants: OK (990 checks, 2 leaders, 95 committed entries)
linearizability: OK
converged: yes
```

The stranded term-1 leader kept accepting writes it could never commit. After the heal it steps
down, and the term-2 leader overwrites those entries.

## Features

- **Leader election** with randomized timeouts, the up-to-date-log vote restriction, and a
  no-op entry appended by each new leader so it can commit entries left over from earlier terms.
- **Log replication**: batched AppendEntries, the consistency check, conflict truncation, and
  the fast-backtracking optimization (followers return `conflictTerm`/`conflictIndex`, so the
  leader skips a whole term per round trip). Commit index advances by majority match, and only
  for entries from the current term (§5.4.2).
- **Crash/restart with persistent state.** Term, vote, log and snapshot live in a
  `PersistentState` "disk" that outlives a crash. Everything else is rebuilt on restart.
- **Log compaction**: nodes snapshot the state machine every N applied entries. Leaders send
  `InstallSnapshot` to followers whose next entry was already compacted away.
- **Replicated KV store** (`PUT`, `APPEND`, `DELETE`, `GET`, with reads going through the log).
  Per-client sessions make retries exactly-once, and sessions are part of the snapshot.
- **Simulator**: a priority-queue event loop with FIFO tie-breaking, cancellable timers, a lossy
  network (per-message random delay, drops, duplicates, symmetric/one-way link cuts, partitions)
  and a Jepsen-style `Nemesis` that injects random faults.
- **Checkers**: an online invariant checker for the four Figure-3 safety properties, plus a
  Wing & Gong style linearizability checker over the client-observed history.
- **CLI** that runs a scenario and prints a timeline, the final state and the check results.

## Build, test, run

Requires JDK 21 and Maven 3.9+.

```sh
mvn test                          # 1571 tests: unit tests + 1500 randomized fault-injection runs
mvn test -Dtest=ChaosTest -Dchaos.seeds=20000   # a much bigger sweep (about 30k runs, ~30s)

mvn package -DskipTests
java -jar target/jraft-sim-0.1.0.jar --list
java -jar target/jraft-sim-0.1.0.jar --scenario chaos --seed 42
java -jar target/jraft-sim-0.1.0.jar -s crash -n 3 -d 4000 --verbose   # include every client op
```

| Scenario    | What it does |
|-------------|--------------|
| `election`  | elect a leader, crash it, watch a new one take over, restart the old one |
| `partition` | isolate the leader in a minority while clients write, then heal |
| `crash`     | crash and restart followers and the leader under load; state comes back from disk |
| `snapshot`  | keep a follower down while the log is compacted; it catches up via InstallSnapshot |
| `chaos`     | random crashes, restarts, partitions and leader isolation on a lossy network |

The CLI exits 0 when every check passes and 1 when one fails, so it can be scripted over seeds.

## Invariants checked

`InvariantChecker` is fed by `RaftListener` callbacks and by a sweep every 100 simulated ms.
It covers the safety properties from Figure 3 of the Raft paper:

| Property | How it is checked |
|---|---|
| **Election safety**: at most one leader per term | every `became leader` event is recorded per term. A second id for the same term is a violation. |
| **Log matching**: same index and term means identical prefixes | pairwise sweep over every node's durable log, crashed nodes included. Find the highest index where two logs agree on the term, then require identical entries up to it. |
| **Leader completeness**: committed entries appear in every later leader's log | each entry is recorded with the term of the leader that committed it. Every newly elected leader, every current leader at each sweep, and every leader of a higher term at commit time must hold it (or have compacted past it). |
| **State machine safety**: no two nodes apply different entries at one index | every apply is compared against the committed record and against what other nodes applied at that index. Applying something no leader committed is also flagged. |

On top of that, every chaos run must be **linearizable** from the clients' point of view, must
finish every client operation once faults stop, and all replicas must **converge** to the same
commit index and identical KV data.

`LinearizabilityChecker` checks each key independently, since linearizability is compositional.
It searches for an order of operations that respects real time and replays correctly against a
sequential register, memoizing (linearized set, state) pairs. An operation that never returned
may take effect at any point after its invocation, or never. Client values are unique
(`c<client>.<seq>;`), and `APPEND` concatenates them, so a lost, duplicated or reordered write
shows up in later reads.

### The checks are not vacuous

`InvariantCheckerTest` and `LinearizabilityCheckerTest` feed hand-built violations to both
checkers. Mutation testing against the chaos suite shows what they catch:

- Dropping the up-to-date check from vote granting fails 129 of 210 chaos runs (leader
  completeness, then committed entries being overwritten).
- Letting leaders commit previous-term entries by counting replicas (the Figure 8 bug) is **not**
  caught, even across 15,000 seeds. Raft only goes wrong there under a very specific sequence of
  crashes and elections, and the leader no-op makes that sequence rarer still. A targeted
  scenario test for Figure 8 would be a good next step.

## Design notes

**Determinism.** The simulator is single-threaded. Events are ordered by `(time, sequence)`, and
every random choice (network delays, drops, election timeouts, client operations, faults) goes
through `Simulator.random()`. `DeterminismTest` checks that two runs with the same seed produce
byte-identical timelines and histories. So a failing chaos seed is a complete bug report:
re-run it and add logging.

**Nodes are event handlers, not threads.** `RaftNode` reacts to messages and timer events and
needs no locks. Each timer is tied to the node's *incarnation*, so timers armed before a crash
can never fire into the restarted node.

**Durability model.** `PersistentState` mutations count as synchronously durable, the moral
equivalent of fsync before replying. Term and vote are persisted before a vote is granted, and
log entries before an AppendEntries is acknowledged. A crash wipes role, commit index, leader
state and the state machine. On restart the node restores its snapshot and sets
`commitIndex = lastApplied = snapshotIndex`, then relearns the rest from the leader.

**Network semantics.** A message gets an independent delay in `[minDelay, maxDelay]` (default
1–15 ms), so reordering is the normal case. The default profile also drops 5% and duplicates 2%
of messages. Cut links drop messages at send time *and* delivery time, so a partition also
swallows traffic already in flight. Clients are not part of node partitions, which lets a
stranded leader keep accepting writes it can never commit.

**Commit rule details.** A follower advances its commit index to
`min(leaderCommit, index of the last entry this message verified)`, not to the end of its log,
because entries past what the message proved could be leftovers from a dead leader. Leaders track
`matchIndex` monotonically, because replies arrive out of order.

**Snapshots.** `RaftLog` stores entries after a base `(snapshotIndex, snapshotTerm)`. The empty
log is base `(0, 0)`, so the first AppendEntries needs no special case. Compaction writes the
snapshot first and then truncates the prefix. A follower that installs a snapshot keeps its log
suffix if the entry at `lastIncludedIndex` matches, and otherwise discards the whole log.
AppendEntries that overlap a follower's snapshot are trimmed to the part beyond it.

**Client protocol.** A client has one operation outstanding and retries it under the same
`(clientId, seq)`, following leader hints and rotating nodes on timeout. Retries may land in the
log more than once. The KV session table makes the second application a no-op that returns the
cached result. That is also why `GET`s that were retried still linearize at their first apply.

## Layout

```
sim/      Simulator (event loop, timers, seeded RNG), Network (delay/drop/dup/partitions)
raft/     RaftNode, RaftLog, PersistentState, Message (sealed RPC hierarchy), RaftConfig
kv/       Command (sealed), KvStore (state machine + client sessions + snapshot codec)
check/    InvariantChecker, History, LinearizabilityChecker
harness/  Cluster (wires everything up), KvClient, Nemesis (fault injector), Timeline
cli/      Main, Scenario
```

## Limitations

- Membership is static: no joint consensus or single-server membership changes.
- No PreVote or leader leases, and reads go through the log rather than ReadIndex.
- A snapshot is sent as one message, not in chunks.
- Storage is modelled, not real: there are no torn writes or lost fsyncs.

## License

MIT, see [LICENSE](LICENSE).
