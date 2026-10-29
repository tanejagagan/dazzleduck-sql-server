package io.dazzleduck.sql.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Properties;

/**
 * Default {@code memory_limit} for a DuckDB that shares its process with the JVM.
 *
 * <p>Left unset, DuckDB sizes its buffer pool from the memory it can see -- in a container, the
 * cgroup limit -- as if it had the box to itself. It does not: the JVM's heap, Metaspace, thread
 * stacks, agents and Arrow's off-heap buffers live in the same process, under the same limit, so
 * DuckDB's default plus the JVM can exceed the container's limit and the kernel OOM-kills the
 * process before DuckDB thinks it needs to spill. For example, a 2 GiB container whose JVM has a
 * 512 MiB max heap gets 2048 - 512 - 512 = 1024 MiB with the default reserve.
 *
 * <p>So the default is what is actually left for DuckDB:
 * <pre>memory_limit = total - max heap - reserve</pre>
 * where {@code total} is the JVM's view of the machine's memory (the container limit under
 * {@code -XX:+UseContainerSupport}, the JVM default), {@code max heap} is
 * {@link Runtime#maxMemory()} (whatever {@code -Xmx} or {@code MaxRAMPercentage} resolved to) and
 * {@code reserve} covers everything else outside the heap (default 512 MiB). Raise the pod limit or
 * the heap and the budget follows; nothing has to be kept in step by hand.
 *
 * <p>Precedence: a {@code memory_limit} in {@code duckdb.properties} wins, and a startup script's
 * {@code SET memory_limit} later overrides either. {@code -Ddazzleduck.duckdb.auto_memory_limit=false}
 * turns the computation off (DuckDB's own default applies), and
 * {@code -Ddazzleduck.duckdb.memory_reserve_mib=N} changes the reserve.
 */
final class DuckDBMemoryLimit {

    private static final Logger logger = LoggerFactory.getLogger(DuckDBMemoryLimit.class);

    static final String MEMORY_LIMIT = "memory_limit";
    static final String AUTO_PROPERTY = "dazzleduck.duckdb.auto_memory_limit";
    static final String RESERVE_PROPERTY = "dazzleduck.duckdb.memory_reserve_mib";
    static final long DEFAULT_RESERVE_MIB = 512;
    /** Below this DuckDB cannot do useful work; a smaller budget means the process is undersized. */
    static final long MIN_LIMIT_MIB = 128;

    private DuckDBMemoryLimit() {}

    /** Sets {@code memory_limit} in {@code properties} unless it is configured or the default is turned off. */
    static void applyDefault(Properties properties) {
        if (properties.containsKey(MEMORY_LIMIT)) {
            return;
        }
        if (!Boolean.parseBoolean(System.getProperty(AUTO_PROPERTY, "true"))) {
            return;
        }
        long total = totalMemoryBytes();
        long maxHeap = Runtime.getRuntime().maxMemory();
        if (total <= 0 || maxHeap == Long.MAX_VALUE) {
            // No usable figure for one side of the subtraction: leave DuckDB's own default.
            logger.warn("DuckDB memory_limit left at DuckDB's default: total memory {} / max heap {} unknown",
                    total, maxHeap);
            return;
        }
        long reserveMib = Long.getLong(RESERVE_PROPERTY, DEFAULT_RESERVE_MIB);
        long limitMib = computeLimitMib(total, maxHeap, reserveMib);
        if (limitMib == MIN_LIMIT_MIB) {
            logger.warn("DuckDB memory_limit floored at {} MiB: {} MiB total - {} MiB heap - {} MiB reserve"
                            + " leaves less; the process is undersized for its heap",
                    MIN_LIMIT_MIB, total >> 20, maxHeap >> 20, reserveMib);
        } else {
            logger.info("DuckDB memory_limit {} MiB = {} MiB total - {} MiB heap - {} MiB reserve",
                    limitMib, total >> 20, maxHeap >> 20, reserveMib);
        }
        properties.setProperty(MEMORY_LIMIT, limitMib + "MiB");
    }

    /** {@code total - maxHeap - reserve} in MiB, never below {@link #MIN_LIMIT_MIB}. */
    static long computeLimitMib(long totalBytes, long maxHeapBytes, long reserveMib) {
        long budget = (totalBytes >> 20) - (maxHeapBytes >> 20) - reserveMib;
        return Math.max(budget, MIN_LIMIT_MIB);
    }

    /** The JVM's view of the machine's memory: the cgroup limit in a container, else physical RAM. */
    private static long totalMemoryBytes() {
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            return os.getTotalMemorySize();
        }
        return -1;
    }
}
