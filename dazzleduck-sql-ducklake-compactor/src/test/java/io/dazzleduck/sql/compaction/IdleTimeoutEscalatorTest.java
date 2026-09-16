package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure policy tests for {@link IdleTimeoutEscalator} — no database, no {@link CompactionService}.
 * Detection is intentionally conservative (see the class javadoc): a failure with no Postgres text
 * anywhere in its cause chain must NOT escalate, since erring toward under-matching is the safer
 * failure mode (a missed escalation just means today's existing behavior).
 */
class IdleTimeoutEscalatorTest {

    private static final String DB = "mylake";
    // No METADATA_PATH ':memory:' — verified incompatible with a same-process DETACH/re-ATTACH,
    // which is exactly what escalation does (see IdleTimeoutEscalationIntegrationTest).
    private static final PostgresMetadataConfig METADATA =
            new PostgresMetadataConfig(DB, "host=pg port=5432 dbname=ducklake user=duck password=duck",
                    "(DATA_PATH 's3://bucket/data')");

    private static CompactionConfig config(boolean adaptive, Duration baseline, Duration max,
                                            Map<String, PostgresMetadataConfig> metadata) {
        return new CompactionConfig(
                List.of(DB), Duration.ofMinutes(1), Duration.ofHours(1), Duration.ofMinutes(5),
                8_000_000L, 64_000_000L, Duration.ofMinutes(15), 8080,
                baseline, max, adaptive, metadata);
    }

    private static IllegalStateException idleTimeoutFailure() {
        return new IllegalStateException("terminating connection due to idle-in-transaction timeout");
    }

    @Test
    void noEscalationWhenFeatureIsOff() {
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(false, Duration.ofMinutes(2), Duration.ofMinutes(30), Map.of(DB, METADATA)));
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure()).isEmpty());
    }

    @Test
    void noEscalationWithoutAPostgresMetadataEntryForTheDatabase() {
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(true, Duration.ofMinutes(2), Duration.ofMinutes(30), Map.of()));
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure()).isEmpty());
    }

    @Test
    void noEscalationWhenTheFailureDoesNotLookIdleTimeoutShaped() {
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(true, Duration.ofMinutes(2), Duration.ofMinutes(30), Map.of(DB, METADATA)));
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, new RuntimeException("disk full")).isEmpty());
    }

    @Test
    void theRollbackVariantWithNoPostgresTextIsNotDetected() {
        // Real observed failure shape (see #426): the cause chain carries no Postgres-specific text
        // at all, so message-substring matching cannot and must not catch it.
        Exception variantA = new RuntimeException(
                "TransactionContext Error: Failed to commit: Failed to execute query \"ROLLBACK\": ");
        assertFalse(IdleTimeoutEscalator.isIdleInTransactionTimeout(variantA));

        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(true, Duration.ofMinutes(2), Duration.ofMinutes(30), Map.of(DB, METADATA)));
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, variantA).isEmpty());
    }

    @Test
    void detectionWalksTheFullCauseChain() {
        Exception wrapped = new RuntimeException("Failed to prepare COPY \"...\"",
                new java.sql.SQLException("SSL connection has been closed unexpectedly"));
        assertTrue(IdleTimeoutEscalator.isIdleInTransactionTimeout(wrapped));
    }

    @Test
    void firstMatchedFailureEscalatesToTheConfiguredBaseline() {
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(true, Duration.ofMillis(100), Duration.ofMillis(1000), Map.of(DB, METADATA)));

        Optional<String> script = escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure());
        assertTrue(script.isPresent());
        assertTrue(script.get().contains("DETACH \"mylake\";"), script.get());
        assertTrue(script.get().contains("ducklake:postgres:"),
                "the postgres: sub-scheme is required, or DuckLake silently falls back to a local "
                        + "file catalog instead of real Postgres metadata (verified empirically): " + script.get());
        assertTrue(script.get().contains("idle_in_transaction_session_timeout=100ms"), script.get());
        assertTrue(script.get().contains("AS mylake (DATA_PATH 's3://bucket/data')"), script.get());
        assertTrue(script.get().endsWith(";"), "must end with ';' so ConnectionPool.splitStatements sees the last statement");
    }

    @Test
    void secondMatchedFailureDoublesAndSubsequentFailuresCapAtTheCeiling() {
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(
                config(true, Duration.ofMillis(100), Duration.ofMillis(350), Map.of(DB, METADATA)));

        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure())
                .orElseThrow().contains("=100ms"));
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure())
                .orElseThrow().contains("=200ms"));
        // 200 * 2 = 400, capped at the configured 350ms ceiling
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure())
                .orElseThrow().contains("=350ms"));
        // Already at the ceiling: no further escalation
        assertTrue(escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure()).isEmpty());
    }

    @Test
    void escalationIsPerDatabase() {
        String otherDb = "otherlake";
        PostgresMetadataConfig otherMetadata = new PostgresMetadataConfig(
                otherDb, "host=pg2 port=5432 dbname=ducklake2 user=duck password=duck", "(DATA_PATH 's3://bucket/other')");
        IdleTimeoutEscalator escalator = new IdleTimeoutEscalator(new CompactionConfig(
                List.of(DB, otherDb), Duration.ofMinutes(1), Duration.ofHours(1), Duration.ofMinutes(5),
                8_000_000L, 64_000_000L, Duration.ofMinutes(15), 8080,
                Duration.ofMillis(100), Duration.ofMillis(1000), true,
                Map.of(DB, METADATA, otherDb, otherMetadata)));

        escalator.reattachScriptIfEscalationNeeded(DB, idleTimeoutFailure());
        // The other database has never failed, so it still escalates from the baseline.
        assertTrue(escalator.reattachScriptIfEscalationNeeded(otherDb, idleTimeoutFailure())
                .orElseThrow().contains("=100ms"));
    }
}
