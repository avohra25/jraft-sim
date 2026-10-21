package io.github.avohra25.jraft.cli;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import io.github.avohra25.jraft.harness.Cluster;
import io.github.avohra25.jraft.raft.RaftNode;
import io.github.avohra25.jraft.sim.Network;

/** {@code jraft-sim [--scenario NAME] [--seed N] [--nodes N] [--duration MS] [--verbose]} */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /** Runs the CLI and returns the process exit code (0 ok, 1 safety failure, 2 usage error). */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        Scenario scenario = Scenario.ELECTION;
        long seed = 1;
        int nodes = 5;
        long duration = 6_000;
        boolean verbose = false;
        Iterator<String> it = Arrays.asList(args).iterator();
        try {
            while (it.hasNext()) {
                String arg = it.next();
                switch (arg) {
                    case "-s", "--scenario" -> scenario = Scenario.valueOf(value(it, arg).toUpperCase(Locale.ROOT));
                    case "--seed" -> seed = Long.parseLong(value(it, arg));
                    case "-n", "--nodes" -> nodes = Integer.parseInt(value(it, arg));
                    case "-d", "--duration" -> duration = Long.parseLong(value(it, arg));
                    case "-v", "--verbose" -> verbose = true;
                    case "-l", "--list" -> {
                        for (Scenario s : Scenario.values()) {
                            out.printf("  %-10s %s%n", s.cliName(), s.description());
                        }
                        return 0;
                    }
                    case "-h", "--help" -> {
                        usage(out);
                        return 0;
                    }
                    default -> throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            if (nodes < 1 || nodes > 9 || duration < 1_000) {
                throw new IllegalArgumentException("need 1-9 nodes and a duration of at least 1000ms");
            }
        } catch (IllegalArgumentException e) {
            err.println("jraft-sim: " + e.getMessage());
            usage(err);
            return 2;
        }

        out.printf("scenario %s: %s%n", scenario.cliName(), scenario.description());
        out.printf("seed %d, %d nodes, %dms of simulated time%n%n", seed, nodes, duration);
        Scenario.Report report = scenario.run(new Scenario.Options(seed, nodes, duration, verbose));
        Cluster cluster = report.cluster();
        out.print(cluster.timeline().render());
        out.println();
        printSummary(out, report);
        return report.ok() ? 0 : 1;
    }

    private static void printSummary(PrintStream out, Scenario.Report report) {
        Cluster cluster = report.cluster();
        Network<?> net = cluster.network();
        out.println("final state");
        for (RaftNode node : cluster.nodes()) {
            Map<String, String> data = cluster.kv(node.id()).data();
            out.printf("  n%d  %-9s term %-3d commit %-5d log %d..%d  kv %s%n", node.id(), node.role(),
                    node.currentTerm(), node.commitIndex(), node.log().firstIndex(), node.log().lastIndex(),
                    abbreviate(data.toString()));
        }
        out.printf("network: %d sent, %d delivered, %d dropped; %d events simulated%n",
                net.stat("sent"), net.stat("delivered"), net.stat("dropped"), cluster.sim().eventsExecuted());
        out.printf("clients: %d operations completed, %d pending%n",
                cluster.history().completedCount(), cluster.history().pendingCount());
        var checker = cluster.checker();
        out.printf("invariants: %s (%d checks, %d leaders, %d committed entries)%n",
                checker.ok() ? "OK" : "VIOLATED", checker.checks(), checker.leadersObserved(),
                checker.committedEntries());
        checker.violations().forEach(v -> out.println("  ! " + v));
        out.printf("linearizability: %s%n", report.linearizability().linearizable() ? "OK"
                : "FAILED - " + report.linearizability().explanation());
        out.printf("converged: %s%n", cluster.converged() ? "yes" : "NO");
    }

    private static String abbreviate(String s) {
        return s.length() <= 60 ? s : s.substring(0, 57) + "...";
    }

    private static String value(Iterator<String> it, String flag) {
        if (!it.hasNext()) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return it.next();
    }

    private static void usage(PrintStream out) {
        out.println("""
                usage: jraft-sim [options]
                  -s, --scenario NAME   election | partition | crash | snapshot | chaos (default election)
                      --seed N          RNG seed; the same seed replays the exact same run (default 1)
                  -n, --nodes N         cluster size, 1-9 (default 5)
                  -d, --duration MS     simulated milliseconds of workload (default 6000)
                  -v, --verbose         also show every completed client operation
                  -l, --list            list scenarios""");
    }
}
