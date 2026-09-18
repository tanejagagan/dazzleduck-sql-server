package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

public class DuckDbTierCompactor implements TierCompactor {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbTierCompactor.class);

    private final String startupScript;
    private final CompactionState metrics;

    // One real DuckDB instance per (database, tier) pair — see RawConnections for why this can't be
    // a duplicated/shared connection (GLOBAL-scoped settings like memory_limit would leak across
    // tiers). Keyed by BOTH database and tier, not tier alone: with multiple databases configured,
    // CompactionService deliberately sizes its thread pool so the same tier can run concurrently for
    // different databases, and a plain JDBC Connection is not safe for concurrent use by more than
    // one thread — keying by tier name only would hand two such threads the same Connection object.
    // Opened lazily on first use and reused for the pair's whole lifetime.
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    public DuckDbTierCompactor(String startupScript, CompactionState metrics) {
        this.startupScript = startupScript;
        this.metrics = metrics;
    }

    @Override
    public MergeOutcome compact(String database, CompactionTier tier) throws Exception {
        Connection connection = connectionFor(database, tier);
        String sql = mergeAdjacentFilesSql(database, tier.minFileSize(), tier.maxFileSize(), tier.maxCompactedFiles());
        Timer.Sample sample = metrics.startTimer();
        long start = System.nanoTime();
        Long compactedFiles = null;
        try (Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                try (var rs = statement.getResultSet()) {
                    compactedFiles = readCompactedFiles(rs, tier);
                }
            }
        } finally {
            metrics.stopTimer(sample, tier.name(), "merge", database);
        }
        long durationMergeMs = (System.nanoTime() - start) / 1_000_000;
        logger.debug("Tier '{}' merge completed for {} in {}ms (compactedFiles={})",
                tier.name(), database, durationMergeMs, compactedFiles);
        // No separate commit time: merge + catalog commit are one atomic CALL here (see CompactionRun).
        return new MergeOutcome(durationMergeMs, compactedFiles);
    }

    // Logged once so the actual ducklake_merge_adjacent_files result shape (spec Q2) can be confirmed
    // against a live DuckLake+catalog, since it can't be verified in unit tests.
    private volatile boolean loggedMergeResultShape = false;

    /**
     * Best-effort {@code compacted_files} from the merge's result set. The function's exact result
     * shape is unconfirmed (spec Q2), so this is <b>provisional</b>: it uses the returned row count as
     * a stand-in for files compacted, only when the merge was bounded ({@code max_compacted_files > 0}),
     * and logs the real shape once at INFO so it can be verified/corrected against a live catalog.
     */
    private Long readCompactedFiles(ResultSet rs, CompactionTier tier) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        long rows = 0;
        String firstRow = null;
        while (rs.next()) {
            if (rows == 0 && !loggedMergeResultShape) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(", ");
                    sb.append(md.getColumnName(i)).append('=').append(rs.getString(i));
                }
                firstRow = sb.toString();
            }
            rows++;
        }
        if (!loggedMergeResultShape) {
            loggedMergeResultShape = true;
            logger.info("ducklake_merge_adjacent_files result shape for tier '{}': {} column(s), {} row(s){}"
                            + " — compactedFiles captured provisionally as the row count (spec Q2; confirm)",
                    tier.name(), cols, rows, firstRow != null ? "; first row: [" + firstRow + "]" : "");
        }
        return tier.maxCompactedFiles() > 0 && rows > 0 ? rows : null;
    }

    private Connection connectionFor(String database, CompactionTier tier) throws SQLException {
        String key = connectionKey(database, tier.name());
        Connection existing = connections.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(key);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, tier.connectionSettings());
            connections.put(key, opened);
            return opened;
        }
    }

    private static String connectionKey(String database, String tierName) {
        return database.length() + ":" + database + "|" + tierName;
    }

    /**
     * {@code min_file_size} is what keeps tiers from racing each other — every tier passes one
     * (always required, including {@code 0} for the lowest tier), fencing it to its own range so
     * tiers can run concurrently with no lock (their ranges are validated disjoint at startup in
     * {@link CompactionConfig#from}). {@code max_compacted_files} is appended only when positive,
     * matching that parameter's own unbounded-at-zero convention.
     */
    static String mergeAdjacentFilesSql(String database, long minFileSizeBytes, long maxFileSizeBytes, long maxCompactedFiles) {
        StringBuilder sql = new StringBuilder("CALL ducklake_merge_adjacent_files('%s', min_file_size := %d, max_file_size := %d"
                .formatted(database, minFileSizeBytes, maxFileSizeBytes));
        if (maxCompactedFiles > 0) {
            sql.append(", max_compacted_files := ").append(maxCompactedFiles);
        }
        return sql.append(")").toString();
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a tier connection", e);
            }
        });
    }
}
