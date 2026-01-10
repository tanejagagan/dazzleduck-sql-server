package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactory;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizerProvider;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory class for creating DuckDBFlightSqlProducer instances from configuration.
 *
 * <p>This utility simplifies the creation of FlightSQL producers by reading all necessary
 * configuration values and creating the producer with appropriate defaults.
 *
 * <h2>Configuration Keys</h2>
 * <ul>
 *   <li><b>flight_sql.host</b> - Host address (default: "0.0.0.0")</li>
 *   <li><b>flight_sql.port</b> - Port number (default: 32010)</li>
 *   <li><b>flight_sql.use_encryption</b> - Whether to use TLS (default: false)</li>
 *   <li><b>warehouse_path</b> - Warehouse directory path (required)</li>
 *   <li><b>secret_key</b> - Secret key for signing (required)</li>
 *   <li><b>producerId</b> - Producer identifier (default: random UUID)</li>
 *   <li><b>access_mode</b> - Access mode: COMPLETE or RESTRICTED (default: COMPLETE)</li>
 *   <li><b>temp_write_location</b> - Temporary write directory (required)</li>
 *   <li><b>query_timeout_minutes</b> - Query timeout in minutes (default: 2)</li>
 *   <li><b>ingestion.min_bucket_size</b> - Minimum ingestion bucket size (default: 1048576)</li>
 *   <li><b>ingestion.max_delay_ms</b> - Maximum ingestion delay in ms (default: 2000)</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * Config config = ConfigFactory.load().getConfig("dazzleduck");
 * DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);
 * }</pre>
 *
 * <h2>Example with Custom Allocator</h2>
 * <pre>{@code
 * Config config = ConfigFactory.load().getConfig("dazzleduck");
 * BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
 * DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
 *     .withAllocator(allocator)
 *     .build();
 * }</pre>
 */
public final class FlightSqlProducerFactory {

    private FlightSqlProducerFactory() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Creates a DuckDBFlightSqlProducer from the provided configuration.
     *
     * <p>This method reads all necessary configuration values and creates a producer
     * with default settings for optional parameters.
     *
     * @param config the configuration object containing producer settings
     * @return a fully configured DuckDBFlightSqlProducer instance
     * @throws IOException if temporary write directory creation fails
     * @throws IllegalArgumentException if required configuration is missing
     * @throws Exception if provider loading fails
     */
    public static DuckDBFlightSqlProducer createFromConfig(Config config) throws Exception {
        return builder(config).build();
    }

    /**
     * Creates a builder for constructing a DuckDBFlightSqlProducer with custom settings.
     *
     * @param config the configuration object containing producer settings
     * @return a ProducerBuilder instance for fluent configuration
     */
    public static ProducerBuilder builder(Config config) {
        return new ProducerBuilder(config);
    }

    /**
     * Builder class for creating DuckDBFlightSqlProducer instances with custom configuration.
     *
     * <p>All configuration values are read eagerly from the config in the constructor.
     * The with* methods can be used to override specific values after construction.
     */
    public static class ProducerBuilder {
        private final Config config;
        private Location location;
        private String producerId;
        private String secretKey;
        private String warehousePath;
        private Path tempWriteDir;
        private AccessMode accessMode;
        private BufferAllocator allocator;
        private PostIngestionTaskFactory postIngestionTaskFactory;
        private QueryOptimizer queryOptimizer;
        private ScheduledExecutorService scheduledExecutorService;
        private Duration queryTimeout;
        private Clock clock;
        private IngestionConfig ingestionConfig;

        private ProducerBuilder(Config config) {
            this.config = config;
            readConfigValues();
        }

        /**
         * Reads all configuration values and sets them as defaults.
         * Called during construction to eagerly load config.
         */
        private void readConfigValues() {
            // Location
            this.location = readLocationFromConfig();

            // Core settings
            this.warehousePath = ConfigUtils.getWarehousePath(config);
            this.secretKey = config.getString(ConfigUtils.SECRET_KEY_KEY);
            this.producerId = config.hasPath("producerId")
                ? config.getString("producerId")
                : UUID.randomUUID().toString();

            // Access mode
            this.accessMode = DuckDBFlightSqlProducer.getAccessMode(config);

            // Temp write directory
            try {
                this.tempWriteDir = DuckDBFlightSqlProducer.getTempWriteDir(config);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temp write directory", e);
            }

            // Query timeout
            this.queryTimeout = config.hasPath("query_timeout_minutes")
                ? Duration.ofMinutes(config.getLong("query_timeout_minutes"))
                : Duration.ofMinutes(2);

            // Ingestion config
            this.ingestionConfig = loadIngestionConfig(config);

            // Load providers (query optimizer, post-ingestion factory)
            try {
                this.queryOptimizer = loadQueryOptimizer(config);
                this.postIngestionTaskFactory = loadPostIngestionTaskFactory(config);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load providers from config", e);
            }

            // Defaults for non-config values
            this.allocator = null; // Will use RootAllocator if not set
            this.scheduledExecutorService = null; // Will create new one if not set
            this.clock = Clock.systemDefaultZone();
        }

        // ==================== Getters for inspecting loaded config ====================

        /**
         * @return the configured location
         */
        public Location getLocation() {
            return location;
        }

        /**
         * @return the configured producer ID
         */
        public String getProducerId() {
            return producerId;
        }

        /**
         * @return the configured warehouse path
         */
        public String getWarehousePath() {
            return warehousePath;
        }

        /**
         * @return the configured access mode
         */
        public AccessMode getAccessMode() {
            return accessMode;
        }

        /**
         * @return the configured query timeout
         */
        public Duration getQueryTimeout() {
            return queryTimeout;
        }

        /**
         * @return the configured ingestion config
         */
        public IngestionConfig getIngestionConfig() {
            return ingestionConfig;
        }

        /**
         * @return the configured query optimizer
         */
        public QueryOptimizer getQueryOptimizer() {
            return queryOptimizer;
        }

        /**
         * @return the configured post-ingestion task factory
         */
        public PostIngestionTaskFactory getPostIngestionTaskFactory() {
            return postIngestionTaskFactory;
        }

        // ==================== Setters for overriding config ====================

        /**
         * Sets a custom location for the Flight server.
         *
         * @param location the Flight server location
         * @return this builder
         */
        public ProducerBuilder withLocation(Location location) {
            this.location = location;
            return this;
        }

        /**
         * Sets a custom producer ID.
         *
         * @param producerId the producer identifier
         * @return this builder
         */
        public ProducerBuilder withProducerId(String producerId) {
            this.producerId = producerId;
            return this;
        }

        /**
         * Sets a custom secret key for signing.
         *
         * @param secretKey the secret key
         * @return this builder
         */
        public ProducerBuilder withSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        /**
         * Sets a custom warehouse path.
         *
         * @param warehousePath the warehouse directory path
         * @return this builder
         */
        public ProducerBuilder withWarehousePath(String warehousePath) {
            this.warehousePath = warehousePath;
            return this;
        }

        /**
         * Sets a custom temporary write directory.
         *
         * @param tempWriteDir the temporary write directory
         * @return this builder
         */
        public ProducerBuilder withTempWriteDir(Path tempWriteDir) {
            this.tempWriteDir = tempWriteDir;
            return this;
        }

        /**
         * Sets a custom access mode.
         *
         * @param accessMode the access mode (COMPLETE or RESTRICTED)
         * @return this builder
         */
        public ProducerBuilder withAccessMode(AccessMode accessMode) {
            this.accessMode = accessMode;
            return this;
        }

        /**
         * Sets a custom buffer allocator.
         *
         * @param allocator the buffer allocator to use
         * @return this builder
         */
        public ProducerBuilder withAllocator(BufferAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        /**
         * Sets a custom post-ingestion task factory.
         *
         * @param factory the post-ingestion task factory
         * @return this builder
         */
        public ProducerBuilder withPostIngestionTaskFactory(PostIngestionTaskFactory factory) {
            this.postIngestionTaskFactory = factory;
            return this;
        }

        /**
         * Sets a custom query optimizer.
         *
         * @param optimizer the query optimizer
         * @return this builder
         */
        public ProducerBuilder withQueryOptimizer(QueryOptimizer optimizer) {
            this.queryOptimizer = optimizer;
            return this;
        }

        /**
         * Sets a custom scheduled executor service.
         *
         * @param executorService the scheduled executor service
         * @return this builder
         */
        public ProducerBuilder withScheduledExecutorService(ScheduledExecutorService executorService) {
            this.scheduledExecutorService = executorService;
            return this;
        }

        /**
         * Sets a custom query timeout.
         *
         * @param timeout the query timeout duration
         * @return this builder
         */
        public ProducerBuilder withQueryTimeout(Duration timeout) {
            this.queryTimeout = timeout;
            return this;
        }

        /**
         * Sets a custom clock for time-based operations.
         *
         * @param clock the clock to use
         * @return this builder
         */
        public ProducerBuilder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Sets a custom ingestion configuration.
         *
         * @param ingestionConfig the ingestion configuration
         * @return this builder
         */
        public ProducerBuilder withIngestionConfig(IngestionConfig ingestionConfig) {
            this.ingestionConfig = ingestionConfig;
            return this;
        }

        /**
         * Builds the DuckDBFlightSqlProducer instance.
         *
         * <p>All configuration values have been read during builder construction.
         * Any values set via with* methods will override the config defaults.
         *
         * @return a fully configured DuckDBFlightSqlProducer
         */
        public DuckDBFlightSqlProducer build() {
            // Use provided allocator or create default
            BufferAllocator finalAllocator = allocator != null
                ? allocator
                : new RootAllocator();

            // Use provided executor or create default
            ScheduledExecutorService finalExecutorService = scheduledExecutorService != null
                ? scheduledExecutorService
                : Executors.newSingleThreadScheduledExecutor();

            // Create appropriate producer based on access mode
            if (accessMode == AccessMode.RESTRICTED) {
                return new RestrictedFlightSqlProducer(
                    location,
                    producerId,
                    secretKey,
                    finalAllocator,
                    warehousePath,
                    tempWriteDir,
                    postIngestionTaskFactory,
                    finalExecutorService,
                    queryTimeout,
                    clock,
                    buildRecorder(producerId),
                    queryOptimizer,
                    ingestionConfig
                );
            } else {
                return new DuckDBFlightSqlProducer(
                    location,
                    producerId,
                    secretKey,
                    finalAllocator,
                    warehousePath,
                    accessMode,
                    tempWriteDir,
                    postIngestionTaskFactory,
                    finalExecutorService,
                    queryTimeout,
                    clock,
                    buildRecorder(producerId),
                        ingestionConfig
                );
            }
        }

        private Location readLocationFromConfig() {
            String host = config.hasPath("flight_sql.host")
                ? config.getString("flight_sql.host") : "0.0.0.0";
            int port = config.hasPath("flight_sql.port")
                ? config.getInt("flight_sql.port") : 32010;
            boolean useEncryption = config.hasPath("flight_sql.use_encryption")
                && config.getBoolean("flight_sql.use_encryption");

            return useEncryption
                ? Location.forGrpcTls(host, port)
                : Location.forGrpcInsecure(host, port);
        }

        private static PostIngestionTaskFactory loadPostIngestionTaskFactory(Config config) throws Exception {
            PostIngestionTaskFactoryProvider provider = ConfigBasedProvider.load(
                config,
                PostIngestionTaskFactoryProvider.POST_INGESTION_CONFIG_PREFIX,
                PostIngestionTaskFactoryProvider.NO_OP
            );
            return provider.getPostIngestionTaskFactory();
        }

        private static QueryOptimizer loadQueryOptimizer(Config config) throws Exception {
            QueryOptimizerProvider provider = ConfigBasedProvider.load(
                config,
                QueryOptimizerProvider.QUERY_OPTIMIZER_PROVIDER_CONFIG_PREFIX,
                QueryOptimizerProvider.NOOPOptimizerProvider
            );
            return provider.getOptimizer();
        }

        private static IngestionConfig loadIngestionConfig(Config config) {
            return IngestionConfig.fromConfig(config.getConfig(IngestionConfig.KEY));
        }

        private static io.dazzleduck.sql.flight.FlightRecorder buildRecorder(String producerId) {
            try {
                var registry = new io.micrometer.core.instrument.logging.LoggingMeterRegistry();
                return new io.dazzleduck.sql.flight.MicroMeterFlightRecorder(registry, producerId);
            } catch (Throwable t) {
                return new NOOPFlightRecorder();
            }
        }
    }
}
