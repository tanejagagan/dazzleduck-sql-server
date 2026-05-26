package io.dazzleduck.sql.commons.ingestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link IngestionHandler} backed by a SQLite registry of queues.
 *
 * <p>A background thread polls {@code schema_version} at {@code checkInterval}. When the
 * version changes it reloads the full registry from {@code ingestion_queues}. New queues
 * are picked up automatically; removed queues are evicted and their
 * {@link ParquetIngestionQueue} is closed.
 *
 * <p>The hot path (every ingest RPC) never touches SQLite — it reads only from the
 * in-memory {@code liveRegistry} map.
 */
public class DynamicIngestionHandler implements IngestionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DynamicIngestionHandler.class);

    private final Connection readConn;
    private final Duration checkInterval;

    private final ConcurrentHashMap<String, QueueIdToTableMapping> liveRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ParquetIngestionQueue> queueCache   = new ConcurrentHashMap<>();
    private final AtomicLong lastSeenVersion = new AtomicLong(-1L);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dynamic-ingestion-reload");
                t.setDaemon(true);
                return t;
            });

    public DynamicIngestionHandler(Connection readConn,
                                   Map<String, QueueIdToTableMapping> initialMappings,
                                   Duration checkInterval) {
        this.readConn      = readConn;
        this.checkInterval = checkInterval;
        liveRegistry.putAll(initialMappings);
        long version = SqliteQueueRepository.readDataVersion(readConn);
        lastSeenVersion.set(version);

        long intervalMs = checkInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::checkAndReload,
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    // -----------------------------------------------------------------------
    // IngestionHandler — hot path (no SQLite I/O)
    // -----------------------------------------------------------------------

    @Override
    public String getTargetPath(String queueId) {
        QueueIdToTableMapping m = liveRegistry.get(queueId);
        return m != null ? m.outputPath() : null;
    }

    @Override
    public String getTransformation(String queueId) {
        QueueIdToTableMapping m = liveRegistry.get(queueId);
        return m != null ? m.transformation() : null;
    }

    @Override
    public String[] getPartitionBy(String queueId) {
        // SQLite registry currently does not store partition columns.
        return new String[0];
    }

    @Override
    public PostIngestionTask createPostIngestionTask(IngestionResult result) {
        QueueIdToTableMapping m = liveRegistry.get(result.queueName());
        if (m == null) return PostIngestionTask.NOOP;
        return new DuckLakePostIngestionTask(result, m.catalog(), m.table(), m.schema(),
                m.additionalParameters());
    }

    @Override
    public Set<String> getKnownQueues() {
        return liveRegistry.keySet();
    }

    // -----------------------------------------------------------------------
    // Queue lifecycle
    // -----------------------------------------------------------------------

    @Override
    public ParquetIngestionQueue getOrCreateQueue(String queueId,
                                                  QueueCreator creator,
                                                  QueueEventListener listener) {
        String path = getTargetPath(queueId);
        if (path == null) {
            ParquetIngestionQueue removed = queueCache.remove(queueId);
            if (removed != null) {
                listener.onDeleted(queueId);
                try { removed.close(); } catch (Exception e) {
                    logger.warn("Failed to close evicted queue: {}", queueId, e);
                }
            }
            return null;
        }
        return queueCache.compute(queueId, (id, existing) -> {
            if (existing != null) return existing;
            ParquetIngestionQueue q = creator.create(id, path);
            listener.onCreated(id);
            return q;
        });
    }

    @Override
    public List<Stats> getQueueStats() {
        return queueCache.values().stream()
                .map(ParquetIngestionQueue::getStats)
                .toList();
    }

    @Override
    public void closeQueues() {
        scheduler.shutdownNow();
        queueCache.forEach((id, queue) -> {
            try { queue.close(); } catch (Exception e) {
                logger.warn("Failed to close ingestion queue: {}", id, e);
            }
        });
        queueCache.clear();
        try { readConn.close(); } catch (SQLException e) {
            logger.warn("Failed to close SQLite read connection", e);
        }
    }

    // -----------------------------------------------------------------------
    // Background reload
    // -----------------------------------------------------------------------

    private void checkAndReload() {
        try {
            long currentVersion = SqliteQueueRepository.readDataVersion(readConn);
            if (currentVersion == lastSeenVersion.get()) return;

            logger.debug("schema_version changed ({} → {}), reloading ingestion queues",
                    lastSeenVersion.get(), currentVersion);

            Map<String, QueueIdToTableMapping> fresh = SqliteQueueRepository.loadAll(readConn);
            applyReload(fresh);
            lastSeenVersion.set(currentVersion);
        } catch (Exception e) {
            logger.warn("Failed to reload ingestion queue registry from SQLite", e);
        }
    }

    private void applyReload(Map<String, QueueIdToTableMapping> fresh) {
        // Add / update entries
        liveRegistry.putAll(fresh);

        // Evict entries that were deleted from the registry
        liveRegistry.keySet().removeIf(id -> {
            if (fresh.containsKey(id)) return false;
            ParquetIngestionQueue q = queueCache.remove(id);
            if (q != null) {
                try { q.close(); } catch (Exception e) {
                    logger.warn("Failed to close evicted queue '{}' during reload", id, e);
                }
            }
            logger.info("Ingestion queue '{}' removed from live registry", id);
            return true;
        });

        // Log newly added queues
        fresh.keySet().forEach(id -> {
            if (!queueCache.containsKey(id)) {
                logger.info("Ingestion queue '{}' → {} registered", id,
                        fresh.get(id).outputPath());
            }
        });
    }
}
