import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class MainTest {
    public static void main(String[] args) throws IOException {
        testParseCsvLine();
        testReadRosterMissingKeys();
        System.out.println("MainTest: all tests passed.");
    }

    private static void testParseCsvLine() {
        List<String> fields = Main.parseCsvLine("alpha,\"bravo, charlie\",\"d\"\"e\"");
        assertEquals(3, fields.size(), "parseCsvLine size");
        assertEquals("alpha", fields.get(0), "parseCsvLine field 0");
        assertEquals("bravo, charlie", fields.get(1), "parseCsvLine field 1");
        assertEquals("d\"e", fields.get(2), "parseCsvLine field 2");
    }

    private static void testReadRosterMissingKeys() throws IOException {
        Path temp = Files.createTempFile("roster-missing-keys", ".csv");
        List<String> lines = List.of(
                "email,cohort,name",
                ",Spring,NoEmail",
                "test@example.com,,MissingCohort",
                "ok@example.com,Fall,Ok"
        );
        Files.write(temp, lines, StandardCharsets.UTF_8);

        Main.Roster roster = Main.readRoster(temp, List.of("email", "cohort"), "none");
        assertEquals(1, roster.rows().size(), "readRoster valid rows");
        assertEquals(2, roster.invalid(), "readRoster invalid count");
        assertEquals(1, roster.missingKeyCounts().getOrDefault("email", 0), "missing email count");
        assertEquals(1, roster.missingKeyCounts().getOrDefault("cohort", 0), "missing cohort count");
    }

    private static void assertEquals(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + " expected " + expected + " but got " + actual);
        }
    }

    private static void assertEquals(String expected, String actual, String label) {
        if (!expected.equals(actual)) {
            throw new AssertionError(label + " expected \"" + expected + "\" but got \"" + actual + "\"");
        }
    }
}
