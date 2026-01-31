package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.sql.FlightSqlProducer;

/**
 * Combined interface for Flight SQL producers that support both gRPC and HTTP access.
 *
 * <p>This interface extends:
 * <ul>
 *   <li>{@link FlightSqlProducer} - Arrow Flight SQL protocol support</li>
 *   <li>{@link HttpFlightAdaptor} - HTTP/REST API support</li>
 *   <li>{@link AutoCloseable} - Resource cleanup</li>
 * </ul>
 *
 * <p>Implementations of this interface can be used with:
 * <ul>
 *   <li>DuckDBProcessor for distributed query execution</li>
 *   <li>AsyncFlightServer for gRPC-based access</li>
 *   <li>HTTP servers for REST API access</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * FlightSqlHttpProducer producer = FlightSqlProducerFactory.builder(config)
 *     .withLocation(location)
 *     .build();
 *
 * DuckDBProcessor processor = new DuckDBProcessor(
 *     controllerLocation,
 *     cluster,
 *     user,
 *     password,
 *     executorService,
 *     producer);
 * }</pre>
 *
 * @see DuckDBFlightSqlProducer
 * @see FlightSqlProducerFactory
 */
public interface FlightSqlHttpProducer extends FlightSqlProducer, HttpFlightAdaptor, AutoCloseable {

    /**
     * Returns the unique identifier for this producer instance.
     * This ID is used for routing requests to the correct executor in distributed environments.
     *
     * @return the producer ID
     */
    @Override
    String getProducerId();

    /**
     * Closes this producer and releases all associated resources.
     *
     * @throws Exception if an error occurs during close
     */
    @Override
    void close() throws Exception;
}
