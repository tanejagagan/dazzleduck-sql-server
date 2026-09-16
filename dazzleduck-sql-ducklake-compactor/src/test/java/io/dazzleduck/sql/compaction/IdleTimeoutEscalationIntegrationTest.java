package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies {@link IdleTimeoutEscalator}'s DETACH/re-ATTACH mechanism against a real Postgres-backed
 * DuckLake catalog — the one thing in this feature worth proving live, since a lot of the original
 * GitHub-issue sketch (ALTER DATABASE, postgres_execute passthrough) turned out to be wrong under
 * real testing.
 *
 * <p><b>What this does NOT prove</b> (documented here so nobody assumes more than it verified):
 * whether Postgres actually kills a connection once the escalated {@code
 * idle_in_transaction_session_timeout} elapses. Verified empirically while writing this test — via
 * {@code pg_stat_activity} on a real Postgres container while a DuckDB {@code BEGIN; INSERT;
 * <sleep>; COMMIT;} sat idle for 3x a configured 1-second timeout — DuckLake does not hold its
 * Postgres metadata connection open merely because an outer DuckDB transaction is open; no session
 * ever appeared in {@code idle in transaction} state, and the commit succeeded regardless of the
 * configured timeout. The real failure this feature targets happens strictly inside DuckLake's own
 * merge-commit protocol, during the slow object-storage write between opening and closing its
 * internal Postgres transaction — not reproducible from outside via ordinary SQL without actually
 * triggering a slow real merge, which is impractical for a fast test.
 *
 * <p>What this DOES prove: the exact DETACH/ATTACH script {@link IdleTimeoutEscalator} builds is
 * valid against a real Postgres-backed catalog, and re-running it (as repeated escalations would)
 * leaves the catalog fully functional — proving the mechanism {@link CompactionService} actually
 * executes on a matched failure is safe, not just syntactically plausible. This surfaced two real
 * constraints while writing it, both now documented in {@link PostgresMetadataConfig} and the
 * README:
 * <ul>
 *   <li>{@code ducklake:host=...} with no scheme (the form used elsewhere in this codebase, e.g.
 *   {@code DuckLakePostgresLoadTest}) silently creates a LOCAL file catalog named after the literal
 *   connection string rather than storing metadata in Postgres — confirmed by zero
 *   {@code ducklake_*} tables ever appearing in Postgres, and the stray file appearing in the
 *   working directory. The required form is {@code ducklake:postgres:host=...}.</li>
 *   <li>{@code METADATA_PATH ':memory:'} (used elsewhere in this codebase purely to avoid stray
 *   test files) is incompatible with a same-*process* DETACH/re-ATTACH: confirmed by reproducing
 *   both ways back to back against the same catalog — with {@code ':memory:'} the re-ATTACH loses
 *   the table, without it (the default local metadata cache) the re-ATTACH correctly re-hydrates
 *   from Postgres and the catalog keeps working. A catalog enabling escalation must NOT use
 *   {@code METADATA_PATH ':memory:'}.</li>
 * </ul>
 */
@Tag("slow")
class IdleTimeoutEscalationIntegrationTest {

    private static final String CATALOG = "pg_lake";

    private static PostgreSQLContainer<?> postgres;
    private static Path dataPath;
    private static String connectionString;
    private static String attachOptions;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                .withDatabaseName("ducklake")
                .withUsername("duck")
                .withPassword("duck");
        postgres.start();

        connectionString = "host=%s port=%d dbname=ducklake user=duck password=duck".formatted(
                postgres.getHost(), postgres.getFirstMappedPort());
        dataPath = Files.createTempDirectory("idle-timeout-escalation-test");
        // No METADATA_PATH ':memory:' here — see the class javadoc: it breaks a same-process
        // DETACH/re-ATTACH, which is exactly what escalation does. The default local metadata cache
        // (a small file next to the working directory) is what production should use too.
        attachOptions = "(DATA_PATH '%s')".formatted(dataPath);

        ConnectionPool.executeOnSingleton("INSTALL ducklake; LOAD ducklake;");
        // The postgres: sub-scheme is required here too — the catalog's OWN original ATTACH (what a
        // real startup script would run) must use it, or there is no real Postgres metadata for
        // escalation to reconnect to (verified empirically; see IdleTimeoutEscalator's comment).
        ConnectionPool.executeOnSingleton(
                "ATTACH 'ducklake:postgres:%s' AS %s %s;\nCREATE TABLE %s.main.t (id INT);"
                        .formatted(connectionString, CATALOG, attachOptions, CATALOG));
        ConnectionPool.execute(ConnectionPool.getConnection(), "INSERT INTO %s.main.t VALUES (1)".formatted(CATALOG));
    }

    @AfterAll
    static void tearDown() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    private static PostgresMetadataConfig metadata() {
        return new PostgresMetadataConfig(CATALOG, connectionString, attachOptions);
    }

    private static long rowCount() throws Exception {
        return ConnectionPool.collectFirst(
                "SELECT COUNT(*) FROM %s.main.t".formatted(CATALOG), Long.class);
    }

    @Test
    void escalatedReattachAgainstARealCatalogSucceedsAndLeavesItFunctional() throws Exception {
        CompactionConfig config = new CompactionConfig(
                List.of(CATALOG), Duration.ofMinutes(1), Duration.ofHours(1), Duration.ofMinutes(5),
                8_000_000L, 64_000_000L, Duration.ofMinutes(15), 8080,
                Duration.ofMillis(500), Duration.ofSeconds(2), true, Map.of(CATALOG, metadata()));
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(config);
        Throwable idleTimeoutFailure = new IllegalStateException(
                "terminating connection due to idle-in-transaction timeout");

        assertEquals(1, rowCount(), "sanity check before touching the catalog");

        // First escalation: DETACH + re-ATTACH at the baseline (500ms).
        Optional<String> first = escalator.reattachScriptIfEscalationNeeded(CATALOG, idleTimeoutFailure);
        assertTrue(first.isPresent());
        assertDoesNotThrow(() -> ConnectionPool.executeOnSingleton(first.get()),
                "the exact script CompactionService would run on a matched failure must be valid SQL");

        ConnectionPool.execute(ConnectionPool.getConnection(), "INSERT INTO %s.main.t VALUES (2)".formatted(CATALOG));
        assertEquals(2, rowCount(), "catalog must still be fully readable/writable after the reattach");

        // Second escalation: doubles to 1000ms, exercising a repeat DETACH/ATTACH the same way a
        // second consecutive matched failure would in production.
        Optional<String> second = escalator.reattachScriptIfEscalationNeeded(CATALOG, idleTimeoutFailure);
        assertTrue(second.isPresent());
        assertTrue(second.get().contains("=1000ms"), second.get());
        assertDoesNotThrow(() -> ConnectionPool.executeOnSingleton(second.get()));

        ConnectionPool.execute(ConnectionPool.getConnection(), "INSERT INTO %s.main.t VALUES (3)".formatted(CATALOG));
        assertEquals(3, rowCount(), "catalog must still work after a second reattach");
    }
}
