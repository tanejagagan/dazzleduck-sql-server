package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for FlightSqlProducerFactory.
 *
 * <p>This test class verifies that the factory correctly:
 * <ul>
 *   <li>Reads configuration values with appropriate defaults</li>
 *   <li>Creates producers based on access mode (COMPLETE vs RESTRICTED)</li>
 *   <li>Supports custom components via builder pattern</li>
 *   <li>Handles missing required configuration appropriately</li>
 * </ul>
 */
public class FlightSqlProducerFactoryTest {

    @TempDir
    Path tempDir;

    private Path warehousePath;
    private Path tempWritePath;

    @BeforeEach
    public void setUp() throws Exception {
        warehousePath = Files.createDirectory(tempDir.resolve("warehouse"));
        tempWritePath = Files.createDirectory(tempDir.resolve("temp"));
    }

    @AfterEach
    public void tearDown() {
        // Cleanup handled by @TempDir
    }

    /**
     * Test creating a producer with minimal configuration (all defaults).
     */
    @Test
    public void testCreateFromConfig_WithMinimalConfig() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        assertTrue(producer instanceof DuckDBFlightSqlProducer);
        assertFalse(producer instanceof RestrictedFlightSqlProducer);

        // Clean up
        producer.close();
    }

    /**
     * Test creating a RESTRICTED mode producer.
     */
    @Test
    public void testCreateFromConfig_RestrictedMode() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "RESTRICTED");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        assertTrue(producer instanceof RestrictedFlightSqlProducer);

        // Clean up
        producer.close();
    }

    /**
     * Test creating a producer with custom host and port.
     */
    @Test
    public void testCreateFromConfig_WithCustomHostAndPort() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("flight_sql.host", "127.0.0.1");
        configMap.put("flight_sql.port", 12345);
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        assertEquals(Location.forGrpcInsecure("127.0.0.1", 12345), producer.getExternalLocation());

        // Clean up
        producer.close();
    }

    /**
     * Test creating a producer with encryption enabled.
     */
    @Test
    public void testCreateFromConfig_WithEncryption() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("flight_sql.host", "secure.example.com");
        configMap.put("flight_sql.port", 443);
        configMap.put("flight_sql.use_encryption", true);
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        assertEquals(Location.forGrpcTls("secure.example.com", 443), producer.getExternalLocation());

        // Clean up
        producer.close();
    }

    /**
     * Test creating a producer with custom producer ID.
     */
    @Test
    public void testCreateFromConfig_WithCustomProducerId() throws Exception {
        String customProducerId = "custom-producer-123";

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, customProducerId);
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        assertEquals(customProducerId, producer.getProducerId());

        // Clean up
        producer.close();
    }

    /**
     * Test creating a producer with custom query timeout.
     */
    @Test
    public void testCreateFromConfig_WithCustomQueryTimeout() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("query_timeout_minutes", 10L);
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);
        // Note: There's no getter for query timeout, so we can't verify it directly
        // But we can verify the producer was created successfully

        // Clean up
        producer.close();
    }

    /**
     * Test creating a producer with custom ingestion config.
     */
    @Test
    public void testCreateFromConfig_WithCustomIngestionConfig() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 2097152L);
        configMap.put("ingestion.max_delay_ms", 5000L);

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test that missing secret key throws an exception.
     */
    @Test
    public void testCreateFromConfig_MissingSecretKey() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        assertThrows(Exception.class, () -> FlightSqlProducerFactory.createFromConfig(config));
    }

    /**
     * Test that missing temp_write_location throws an exception.
     */
    @Test
    public void testCreateFromConfig_MissingTempWriteLocation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        assertThrows(Exception.class, () -> FlightSqlProducerFactory.createFromConfig(config));
    }

    /**
     * Test builder pattern with custom allocator.
     */
    @Test
    public void testBuilder_WithCustomAllocator() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        BufferAllocator customAllocator = new RootAllocator(1024 * 1024);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withAllocator(customAllocator)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
        // Note: Don't close customAllocator here as it's owned by the producer
    }

    /**
     * Test builder pattern with custom query optimizer.
     */
    @Test
    public void testBuilder_WithCustomQueryOptimizer() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        QueryOptimizer customOptimizer = QueryOptimizer.NOOP_QUERY_OPTIMIZER;

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withQueryOptimizer(customOptimizer)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test builder pattern with custom clock.
     */
    @Test
    public void testBuilder_WithCustomClock() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        Clock customClock = Clock.systemUTC();

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withClock(customClock)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test builder pattern with custom query timeout.
     */
    @Test
    public void testBuilder_WithCustomQueryTimeout() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        Duration customTimeout = Duration.ofMinutes(15);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withQueryTimeout(customTimeout)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test builder pattern with custom ingestion config.
     */
    @Test
    public void testBuilder_WithCustomIngestionConfig() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        IngestionConfig customIngestionConfig = new IngestionConfig(4096, Duration.ofMillis(1000));

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withIngestionConfig(customIngestionConfig)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test builder pattern with multiple custom components.
     */
    @Test
    public void testBuilder_WithMultipleCustomComponents() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);

        Config config = ConfigFactory.parseMap(configMap);

        BufferAllocator customAllocator = new RootAllocator(1024 * 1024);
        QueryOptimizer customOptimizer = QueryOptimizer.NOOP_QUERY_OPTIMIZER;
        Clock customClock = Clock.systemUTC();
        Duration customTimeout = Duration.ofMinutes(5);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withAllocator(customAllocator)
                .withQueryOptimizer(customOptimizer)
                .withClock(customClock)
                .withQueryTimeout(customTimeout)
                .build();

        assertNotNull(producer);

        // Clean up
        producer.close();
    }

    /**
     * Test that default values are used when optional config is missing.
     */
    @Test
    public void testCreateFromConfig_UsesDefaultValues() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConfigUtils.WAREHOUSE_CONFIG_KEY, warehousePath.toString());
        configMap.put(ConfigUtils.SECRET_KEY_KEY, "test-secret-key");
        configMap.put(ConfigUtils.PRODUCER_ID_KEY, "test-producer");
        configMap.put(ConfigUtils.TEMP_WRITE_LOCATION_KEY, tempWritePath.toString());
        configMap.put(ConfigUtils.ACCESS_MODE_KEY, "COMPLETE");
        configMap.put("ingestion.min_bucket_size", 1048576L);
        configMap.put("ingestion.max_delay_ms", 2000L);
        // Not setting: flight_sql.host, flight_sql.port, use_encryption, query_timeout_minutes

        Config config = ConfigFactory.parseMap(configMap);

        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.createFromConfig(config);

        assertNotNull(producer);

        // Verify defaults
        assertEquals(Location.forGrpcInsecure("0.0.0.0", 32010), producer.getExternalLocation());
        assertEquals("test-producer", producer.getProducerId());
        assertFalse(producer instanceof RestrictedFlightSqlProducer); // Default is COMPLETE mode

        // Clean up
        producer.close();
    }
}
