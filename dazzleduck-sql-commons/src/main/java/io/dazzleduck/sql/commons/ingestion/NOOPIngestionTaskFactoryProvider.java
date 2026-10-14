package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * No-operation implementation of IngestionTaskFactoryProvider.
 * Returns a factory that creates NOOP tasks which do nothing.
 */
public class NOOPIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private Config config;
    private String ingestionPath;

    public NOOPIngestionTaskFactoryProvider(Config config){
        setConfig(config);
    }

    public NOOPIngestionTaskFactoryProvider(String ingestionPath){
        this.ingestionPath = ingestionPath;
    }
    @Override
    public void setConfig(Config config) {
        this.config = config;
        this.ingestionPath = config.getString("ingestion_path");
    }

    /**
     * A new handler, writing each queue's Parquet under {@code ingestion_path/<queue-id>/}. It
     * keeps one queue per ID (see {@link IngestionQueueCache}) and drains them on
     * {@link IngestionHandler#closeQueues}.
     */
    @Override
    public IngestionHandler getIngestionHandler() {
        return new IngestionHandler() {
            private final IngestionQueueCache queues = new IngestionQueueCache();

            @Override
            public ParquetIngestionQueue getOrCreateQueue(String queueId, QueueCreator creator,
                                                          QueueEventListener listener) {
                return queues.getOrCreate(queueId, getTargetPath(queueId), creator, listener);
            }

            @Override
            public List<Stats> getQueueStats() {
                return queues.stats();
            }

            @Override
            public void closeQueues() {
                closeQueues(DEFAULT_DRAIN_TIMEOUT);
            }

            @Override
            public void closeQueues(Duration drainTimeout) {
                queues.closeAll(drainTimeout);
            }

            @Override
            public PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult) {
                return PostIngestionTask.NOOP;
            }

            @Override
            public String getTargetPath(String queueId) {
                return Path.of(ingestionPath).resolve(queueId).toString();
            }

            @Override
            public String[] getPartitionBy(String queueId) {
                return new String[0];
            }
        };

    }
}
