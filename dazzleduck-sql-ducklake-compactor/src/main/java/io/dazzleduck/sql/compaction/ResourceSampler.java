package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Local, catalog-free "resource headroom" readings for a {@link CompactionRun} (spec §Resource
 * headroom). Everything here reads {@code /proc} or the local filesystem, or parses config — no
 * round-trips. Every method returns {@code -1} when a reading is not available (e.g. {@code /proc}
 * is Linux-only; a tier that sets no {@code memory_limit}), so an unknown never masquerades as zero.
 */
final class ResourceSampler {

    private static final Logger logger = LoggerFactory.getLogger(ResourceSampler.class);

    private ResourceSampler() {}

    private static final Path PROC_SELF_STATUS = Path.of("/proc/self/status");
    private static final Pattern VM_HWM = Pattern.compile("^VmHWM:\\s+(\\d+)\\s+kB", Pattern.MULTILINE);
    // e.g. SET memory_limit='2GB';  SET GLOBAL temp_directory = '/tmp/spill'
    private static final Pattern MEMORY_LIMIT =
            Pattern.compile("memory_limit\\s*(?::=|=)?\\s*'?([0-9.]+\\s*[a-zA-Z]*)'?", Pattern.CASE_INSENSITIVE);
    private static final Pattern TEMP_DIRECTORY =
            Pattern.compile("temp_directory\\s*(?::=|=)?\\s*'([^']+)'", Pattern.CASE_INSENSITIVE);

    /** Process peak resident set size in bytes from {@code /proc/self/status} VmHWM, or -1. */
    static long rssPeakBytes() {
        try {
            if (!Files.exists(PROC_SELF_STATUS)) {
                return -1;
            }
            Matcher m = VM_HWM.matcher(Files.readString(PROC_SELF_STATUS));
            return m.find() ? Long.parseLong(m.group(1)) * 1024 : -1;
        } catch (Exception e) {
            logger.debug("Could not read VmHWM", e);
            return -1;
        }
    }

    /** Total bytes currently under {@code dir} (spill/temp usage), or -1 when unknown/missing. */
    static long dirSizeBytes(String dir) {
        if (dir == null || dir.isBlank()) {
            return -1;
        }
        Path root = Path.of(dir);
        if (!Files.isDirectory(root)) {
            return 0; // configured but not yet created == no spill
        }
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        } catch (Exception e) {
            logger.debug("Could not size temp dir {}", dir, e);
            return -1;
        }
    }

    /** {@code memory_limit} in bytes parsed from a tier's connection_settings, or -1 if none set. */
    static long memoryLimitBytes(List<String> connectionSettings) {
        for (String s : connectionSettings) {
            Matcher m = MEMORY_LIMIT.matcher(s);
            if (m.find()) {
                return parseSize(m.group(1).trim());
            }
        }
        return -1;
    }

    /** {@code temp_directory} path from a tier's connection_settings, or null if none set. */
    static String tempDirectory(List<String> connectionSettings) {
        for (String s : connectionSettings) {
            Matcher m = TEMP_DIRECTORY.matcher(s);
            if (m.find()) {
                return m.group(1);
            }
        }
        return null;
    }

    // e.g. "-c idle_in_transaction_session_timeout=600000", "SET idle_in_transaction_session_timeout='10min'"
    private static final Pattern IDLE_TX_TIMEOUT =
            Pattern.compile("idle_in_transaction_session_timeout\\s*(?::=|=)?\\s*'?([0-9]+\\s*[a-zA-Z]*)'?",
                    Pattern.CASE_INSENSITIVE);

    /**
     * The catalog's {@code idle_in_transaction_session_timeout} in ms, parsed from a tier's
     * connection_settings (this is the external timeout a cycle races, and the natural denominator
     * for durationHeadroom), or {@code -1} if the tier does not set one — in which case the caller
     * should fall back to the server default. Bare numbers are milliseconds (the Postgres unit);
     * a unit suffix ({@code ms}/{@code s}/{@code min}/{@code h}) is honoured.
     */
    static long idleInTransactionTimeoutMs(List<String> connectionSettings) {
        for (String s : connectionSettings) {
            Matcher m = IDLE_TX_TIMEOUT.matcher(s);
            if (m.find()) {
                return parseDurationMs(m.group(1).trim());
            }
        }
        return -1;
    }

    /** Parses "600000" / "600000ms" / "10min" / "30s" / "1h" to milliseconds, or -1. */
    static long parseDurationMs(String value) {
        Matcher m = Pattern.compile("([0-9]+)\\s*([a-zA-Z]*)").matcher(value);
        if (!m.matches()) {
            return -1;
        }
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2).toLowerCase(java.util.Locale.ROOT)) {
            case "", "ms" -> n;
            case "s" -> n * 1000;
            case "min", "m" -> n * 60_000;
            case "h" -> n * 3_600_000;
            default -> -1L;
        };
    }

    /** Parses "2GB" / "512 MB" / "1073741824" to bytes (1024-based units), or -1 on a bad value. */
    static long parseSize(String value) {
        Matcher m = Pattern.compile("([0-9.]+)\\s*([a-zA-Z]*)").matcher(value);
        if (!m.matches()) {
            return -1;
        }
        try {
            double n = Double.parseDouble(m.group(1));
            long mult = switch (m.group(2).toUpperCase(java.util.Locale.ROOT)) {
                case "", "B" -> 1L;
                case "KB", "KIB" -> 1024L;
                case "MB", "MIB" -> 1024L * 1024;
                case "GB", "GIB" -> 1024L * 1024 * 1024;
                case "TB", "TIB" -> 1024L * 1024 * 1024 * 1024;
                default -> -1L;
            };
            return mult < 0 ? -1 : (long) (n * mult);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
