package io.github.avohra25.jraft.raft;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import io.github.avohra25.jraft.kv.Command;
import io.github.avohra25.jraft.raft.Message.AppendEntries;
import io.github.avohra25.jraft.raft.Message.AppendEntriesReply;
import io.github.avohra25.jraft.raft.Message.ClientReply;
import io.github.avohra25.jraft.raft.Message.ClientRequest;
import io.github.avohra25.jraft.raft.Message.InstallSnapshot;
import io.github.avohra25.jraft.raft.Message.InstallSnapshotReply;
import io.github.avohra25.jraft.raft.Message.RequestVote;
import io.github.avohra25.jraft.raft.Message.RequestVoteReply;
import io.github.avohra25.jraft.sim.Network;
import io.github.avohra25.jraft.sim.Simulator;

/**
 * One Raft server.
 *
 * <p>The node is an event-driven state machine: every method runs inside a simulator event, so
 * there is no locking. Durable state lives in a {@link PersistentState} that outlives
 * {@link #crash()}; everything else (role, commit index, state machine, replication progress)
 * is volatile and rebuilt by {@link #start()}.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final int id;
    private final List<Integer> peers;
    private final Simulator sim;
    private final Network<Message> network;
    private final RaftConfig config;
    private final PersistentState disk;
    private final Supplier<? extends StateMachine> stateMachineFactory;
    private final RaftListener listener;

    // Volatile state, reset on every (re)start.
    private boolean up;
    private int incarnation;
    private Role role = Role.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex;
    private long lastApplied;
    private StateMachine stateMachine;
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Set<Integer> votes = new HashSet<>();
    private Simulator.Timer electionTimer;
    private Simulator.Timer heartbeatTimer;

    public RaftNode(int id, List<Integer> peers, Simulator sim, Network<Message> network, RaftConfig config,
                    PersistentState disk, Supplier<? extends StateMachine> stateMachineFactory,
                    RaftListener listener) {
        if (peers.contains(id)) {
            throw new IllegalArgumentException("peer list must not contain the node itself");
        }
        this.id = id;
        this.peers = List.copyOf(peers);
        this.sim = sim;
        this.network = network;
        this.config = config;
        this.disk = disk;
        this.stateMachineFactory = stateMachineFactory;
        this.listener = listener;
        network.register(id, envelope -> {
            if (up) {
                handle(envelope.from(), envelope.message());
            }
        });
    }

    // ---------------------------------------------------------------- lifecycle

    /** Boots (or reboots) the node from whatever is on its disk. */
    public void start() {
        if (up) {
            throw new IllegalStateException("node " + id + " is already running");
        }
        up = true;
        incarnation++;
        role = Role.FOLLOWER;
        leaderId = -1;
        votes.clear();
        nextIndex.clear();
        matchIndex.clear();
        stateMachine = stateMachineFactory.get();
        RaftLog log = disk.log();
        if (disk.snapshot() != null) {
            stateMachine.restore(disk.snapshot());
        }
        // Only the snapshot is known to be committed; the rest is relearned from the leader.
        commitIndex = log.snapshotIndex();
        lastApplied = log.snapshotIndex();
        resetElectionTimer();
    }

    /** Fail-stop crash: volatile state is lost, the disk survives. */
    public void crash() {
        if (!up) {
            return;
        }
        up = false;
        cancel(electionTimer);
        cancel(heartbeatTimer);
        Role previous = role;
        role = Role.FOLLOWER;
        leaderId = -1;
        stateMachine = null;
        if (previous != Role.FOLLOWER) {
            listener.onRoleChange(this, previous);
        }
    }

    // ---------------------------------------------------------------- accessors

    public int id() {
        return id;
    }

    public boolean isUp() {
        return up;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return disk.currentTerm();
    }

    public int votedFor() {
        return disk.votedFor();
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    /** The node's log (read it, don't write it: it is the node's durable state). */
    public RaftLog log() {
        return disk.log();
    }

    public StateMachine stateMachine() {
        return stateMachine;
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    private int majority() {
        return clusterSize() / 2 + 1;
    }

    // ---------------------------------------------------------------- timers

    /** Schedules {@code action} for this incarnation only: it is dropped if the node crashes. */
    private Simulator.Timer after(long delay, Runnable action) {
        int scheduledIn = incarnation;
        return sim.schedule(delay, () -> {
            if (up && incarnation == scheduledIn) {
                action.run();
            }
        });
    }

    private static void cancel(Simulator.Timer timer) {
        if (timer != null) {
            timer.cancel();
        }
    }

    private void resetElectionTimer() {
        cancel(electionTimer);
        long timeout = sim.uniform(config.electionTimeoutMin(), config.electionTimeoutMax());
        electionTimer = after(timeout, this::startElection);
    }

    private void scheduleHeartbeat() {
        cancel(heartbeatTimer);
        heartbeatTimer = after(config.heartbeatInterval(), () -> {
            if (role == Role.LEADER) {
                broadcastAppendEntries();
                scheduleHeartbeat();
            }
        });
    }

    // ---------------------------------------------------------------- roles

    private void setRole(Role next) {
        Role previous = role;
        role = next;
        if (previous != next || next == Role.CANDIDATE) {
            listener.onRoleChange(this, previous);
        }
    }

    private void startElection() {
        disk.advanceTerm(disk.currentTerm() + 1);
        disk.voteFor(id);
        leaderId = -1;
        votes.clear();
        votes.add(id);
        setRole(Role.CANDIDATE);
        resetElectionTimer();
        if (votes.size() >= majority()) {
            becomeLeader();
            return;
        }
        RaftLog log = disk.log();
        RequestVote request = new RequestVote(disk.currentTerm(), id, log.lastIndex(), log.lastTerm());
        for (int peer : peers) {
            send(peer, request);
        }
    }

    private void becomeLeader() {
        cancel(electionTimer);
        leaderId = id;
        RaftLog log = disk.log();
        for (int peer : peers) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        setRole(Role.LEADER);
        // A no-op from the new term lets the leader commit (and so learn the fate of) entries
        // left behind by earlier leaders, which it may not commit by counting replicas alone.
        log.append(new LogEntry(disk.currentTerm(), log.lastIndex() + 1, new Command.Noop()));
        advanceCommitIndex();
        broadcastAppendEntries();
        scheduleHeartbeat();
    }

    /** Adopts {@code term} if newer and falls back to follower. */
    private void stepDown(long term) {
        disk.advanceTerm(term);
        if (role != Role.FOLLOWER) {
            boolean wasLeader = role == Role.LEADER;
            cancel(heartbeatTimer);
            setRole(Role.FOLLOWER);
            if (wasLeader) {
                resetElectionTimer();
            }
        }
    }

    // ---------------------------------------------------------------- dispatch

    private void send(int to, Message message) {
        network.send(id, to, message);
    }

    private void handle(int from, Message message) {
        if (message instanceof Message.RaftRpc rpc && rpc.term() > disk.currentTerm()) {
            stepDown(rpc.term());
        }
        switch (message) {
            case RequestVote m -> onRequestVote(from, m);
            case RequestVoteReply m -> onRequestVoteReply(from, m);
            case AppendEntries m -> onAppendEntries(from, m);
            case AppendEntriesReply m -> onAppendEntriesReply(from, m);
            case InstallSnapshot m -> onInstallSnapshot(from, m);
            case InstallSnapshotReply m -> onInstallSnapshotReply(from, m);
            case ClientRequest m -> onClientRequest(from, m);
            case ClientReply m -> { } // servers never receive these
        }
    }

    // ---------------------------------------------------------------- elections

    private boolean candidateLogIsUpToDate(long lastLogTerm, long lastLogIndex) {
        RaftLog log = disk.log();
        if (lastLogTerm != log.lastTerm()) {
            return lastLogTerm > log.lastTerm();
        }
        return lastLogIndex >= log.lastIndex();
    }

    private void onRequestVote(int from, RequestVote m) {
        boolean granted = false;
        if (m.term() == disk.currentTerm()
                && (disk.votedFor() == PersistentState.NO_VOTE || disk.votedFor() == m.candidateId())
                && candidateLogIsUpToDate(m.lastLogTerm(), m.lastLogIndex())) {
            granted = true;
            disk.voteFor(m.candidateId());
            resetElectionTimer();
        }
        send(from, new RequestVoteReply(disk.currentTerm(), granted));
    }

    private void onRequestVoteReply(int from, RequestVoteReply m) {
        if (role != Role.CANDIDATE || m.term() != disk.currentTerm() || !m.granted()) {
            return;
        }
        votes.add(from);
        if (votes.size() >= majority()) {
            becomeLeader();
        }
    }

    // ---------------------------------------------------------------- replication (follower side)

    private void onAppendEntries(int from, AppendEntries m) {
        long term = disk.currentTerm();
        if (m.term() < term) {
            send(from, new AppendEntriesReply(term, false, 0, 0, -1));
            return;
        }
        if (role != Role.FOLLOWER) {
            stepDown(m.term()); // a candidate that hears from this term's leader gives up
        }
        leaderId = m.leaderId();
        resetElectionTimer();

        RaftLog log = disk.log();
        long prevIndex = m.prevLogIndex();
        long prevTerm = m.prevLogTerm();
        List<LogEntry> entries = m.entries();
        if (prevIndex < log.snapshotIndex()) {
            // Our snapshot already covers part of this request; those entries are committed and
            // therefore identical, so only look at what lies beyond the snapshot.
            long base = log.snapshotIndex();
            entries = entries.stream().filter(e -> e.index() > base).toList();
            prevIndex = base;
            prevTerm = log.snapshotTerm();
        }
        if (prevIndex > log.lastIndex()) {
            send(from, new AppendEntriesReply(term, false, 0, log.lastIndex() + 1, -1));
            return;
        }
        if (log.termAt(prevIndex) != prevTerm) {
            long conflictTerm = log.termAt(prevIndex);
            send(from, new AppendEntriesReply(term, false, 0, log.firstIndexOfTermRun(prevIndex), conflictTerm));
            return;
        }
        for (LogEntry entry : entries) {
            if (entry.index() <= log.lastIndex()) {
                if (log.termAt(entry.index()) == entry.term()) {
                    continue; // already have it (duplicate or reordered message)
                }
                if (entry.index() <= commitIndex) {
                    throw new IllegalStateException("node " + id + " asked to overwrite committed index " + entry.index());
                }
                log.truncateFrom(entry.index());
            }
            log.append(entry);
        }
        // Only what this message proved to match may be committed: entries past it could still
        // be leftovers from an older leader.
        long lastNew = prevIndex + entries.size();
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(m.leaderCommit(), lastNew));
        }
        send(from, new AppendEntriesReply(term, true, lastNew, 0, -1));
        applyCommitted();
    }

    // ---------------------------------------------------------------- replication (leader side)

    private void broadcastAppendEntries() {
        for (int peer : peers) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(int peer) {
        RaftLog log = disk.log();
        long next = nextIndex.get(peer);
        if (next <= log.snapshotIndex()) {
            send(peer, new InstallSnapshot(disk.currentTerm(), id, log.snapshotIndex(), log.snapshotTerm(),
                    disk.snapshot()));
            return;
        }
        long prevIndex = next - 1;
        send(peer, new AppendEntries(disk.currentTerm(), id, prevIndex, log.termAt(prevIndex),
                log.slice(next, config.maxEntriesPerAppend()), commitIndex));
    }

    private void onAppendEntriesReply(int from, AppendEntriesReply m) {
        if (role != Role.LEADER || m.term() != disk.currentTerm()) {
            return;
        }
        RaftLog log = disk.log();
        long match = matchIndex.get(from);
        if (m.success()) {
            // Replies can arrive out of order; progress only ever moves forward.
            match = Math.max(match, m.matchIndex());
            matchIndex.put(from, match);
            nextIndex.put(from, Math.max(nextIndex.get(from), match + 1));
            advanceCommitIndex();
            if (nextIndex.get(from) <= log.lastIndex()) {
                sendAppendEntries(from);
            }
            return;
        }
        long next = m.conflictIndex();
        if (m.conflictTerm() >= 0) {
            long last = log.lastIndexOfTerm(m.conflictTerm());
            if (last > 0) {
                next = last + 1;
            }
        }
        next = Math.min(Math.max(next, match + 1), log.lastIndex() + 1);
        nextIndex.put(from, next);
        sendAppendEntries(from);
    }

    private void advanceCommitIndex() {
        RaftLog log = disk.log();
        long term = disk.currentTerm();
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != term) {
                break; // never commit an older term's entry by counting (Raft paper, section 5.4.2)
            }
            int replicas = 1;
            for (int peer : peers) {
                if (matchIndex.get(peer) >= n) {
                    replicas++;
                }
            }
            if (replicas >= majority()) {
                commitIndex = n;
                listener.onLeaderCommit(this, n);
                applyCommitted();
                return;
            }
        }
    }

    private void applyCommitted() {
        RaftLog log = disk.log();
        while (lastApplied < commitIndex) {
            LogEntry entry = log.entry(++lastApplied);
            String result = stateMachine.apply(entry.command());
            listener.onApply(this, entry, result);
            if (role == Role.LEADER && entry.command() instanceof Command.ClientCommand cmd) {
                send(cmd.clientId(), new ClientReply(cmd.clientId(), cmd.seq(), true, result, id));
            }
        }
        maybeSnapshot();
    }

    // ---------------------------------------------------------------- log compaction

    private void maybeSnapshot() {
        RaftLog log = disk.log();
        int threshold = config.snapshotThreshold();
        if (threshold == 0 || lastApplied - log.snapshotIndex() < threshold) {
            return;
        }
        // Write the snapshot before discarding the entries it replaces.
        disk.saveSnapshot(stateMachine.snapshot());
        log.compactThrough(lastApplied);
        listener.onSnapshot(this, lastApplied, false);
    }

    private void onInstallSnapshot(int from, InstallSnapshot m) {
        long term = disk.currentTerm();
        if (m.term() < term) {
            send(from, new InstallSnapshotReply(term, 0));
            return;
        }
        if (role != Role.FOLLOWER) {
            stepDown(m.term());
        }
        leaderId = m.leaderId();
        resetElectionTimer();
        long index = m.lastIncludedIndex();
        if (index > commitIndex) {
            RaftLog log = disk.log();
            if (log.hasTermAt(index) && log.termAt(index) == m.lastIncludedTerm()) {
                log.compactThrough(index); // keep the suffix we already have, it matches
            } else {
                log.resetToSnapshot(index, m.lastIncludedTerm());
            }
            disk.saveSnapshot(m.data());
            stateMachine.restore(m.data());
            commitIndex = index;
            lastApplied = index;
            listener.onSnapshot(this, index, true);
        }
        // Either way everything through `index` is committed, hence identical to the leader's.
        send(from, new InstallSnapshotReply(term, index));
    }

    private void onInstallSnapshotReply(int from, InstallSnapshotReply m) {
        if (role != Role.LEADER || m.term() != disk.currentTerm()) {
            return;
        }
        long match = Math.max(matchIndex.get(from), m.matchIndex());
        matchIndex.put(from, match);
        nextIndex.put(from, Math.max(nextIndex.get(from), match + 1));
        advanceCommitIndex();
        if (nextIndex.get(from) <= disk.log().lastIndex()) {
            sendAppendEntries(from);
        }
    }

    // ---------------------------------------------------------------- clients

    private void onClientRequest(int from, ClientRequest m) {
        Command.ClientCommand cmd = m.command();
        if (role != Role.LEADER) {
            send(from, new ClientReply(cmd.clientId(), cmd.seq(), false, null, leaderId));
            return;
        }
        // Duplicates (client retries) are appended too; the state machine's sessions make
        // re-application a no-op that just returns the original result.
        RaftLog log = disk.log();
        log.append(new LogEntry(disk.currentTerm(), log.lastIndex() + 1, cmd));
        advanceCommitIndex(); // single-node clusters commit immediately
        broadcastAppendEntries();
    }

    @Override
    public String toString() {
        return "n" + id + "[" + role + " t" + disk.currentTerm() + " commit=" + commitIndex
                + " last=" + disk.log().lastIndex() + (up ? "" : " DOWN") + "]";
    }
}
