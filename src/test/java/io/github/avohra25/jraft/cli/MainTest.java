package io.github.avohra25.jraft.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

class MainTest {

    private record Run(int code, String out, String err) {}

    private static Run main(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = Main.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(Scenario.class)
    void everyScenarioRunsCleanly(Scenario scenario) {
        Run run = main("--scenario", scenario.cliName(), "--seed", "3");
        assertEquals(0, run.code(), run.out());
        assertTrue(run.out().contains("became LEADER"), run.out());
        assertTrue(run.out().contains("invariants: OK"), run.out());
        assertTrue(run.out().contains("linearizability: OK"), run.out());
        assertTrue(run.out().contains("converged: yes"), run.out());
    }

    @Test
    void timelineShowsTheInjectedFaults() {
        Run run = main("-s", "partition", "--seed", "9", "-n", "5");
        assertTrue(run.out().contains("PARTITION"), run.out());
        assertTrue(run.out().contains("HEALED"), run.out());
        Run snapshot = main("-s", "snapshot", "--seed", "2");
        assertTrue(snapshot.out().contains("installed snapshot"), snapshot.out());
    }

    @Test
    void verboseListsClientOperations() {
        Run run = main("-s", "crash", "-v", "-d", "2000");
        assertTrue(run.out().contains("c100"), run.out());
    }

    @Test
    void listAndUsageErrors() {
        Run list = main("--list");
        assertEquals(0, list.code());
        assertTrue(list.out().contains("chaos"));
        Run bad = main("--bogus");
        assertEquals(2, bad.code());
        assertTrue(bad.err().contains("unknown argument"));
        assertEquals(2, main("--scenario", "nope").code());
        assertEquals(2, main("--nodes").code());
    }
}
