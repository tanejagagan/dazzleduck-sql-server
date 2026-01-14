package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.PutResult;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Adaptor interface that extends {@link FlightProducer} to provide HTTP-specific
 * functionality for the DazzleDuck SQL server.
 *
 * <p>This interface bridges the Arrow Flight SQL protocol with HTTP-based operations,
 * enabling both gRPC and REST API access to the same underlying query engine.
 */
public interface HttpFlightAdaptor extends FlightProducer {

    /**
     * Accepts a bulk ingestion request via HTTP, processing Arrow data from an input stream.
     *
     * @param context the call context containing peer identity and credentials
     * @param ingestionParameters parameters specifying the ingestion target and options
     * @param inputStream the input stream containing Arrow-formatted data
     * @param ackStream listener for acknowledgment results
     * @return a Runnable that performs the ingestion when executed
     */
    Runnable acceptPutStatementBulkIngest(
            FlightProducer.CallContext context,
            IngestionParameters ingestionParameters,
            InputStream inputStream,
            FlightProducer.StreamListener<PutResult> ackStream);

    /**
     * Returns the unique identifier for this producer instance.
     *
     * @return the producer ID
     */
    String getProducerId();


    /**
     * Attempts to cancel a running query without reporting status.
     *
     * <p>This is a best-effort, fire-and-forget cancellation method designed for
     * high-availability environments where cancellation requests are broadcast
     * to multiple nodes when the actual executor is not known.
     *
     * <p>This method:
     * <ul>
     *   <li>Does not require a listener - no status feedback is provided</li>
     *   <li>Silently returns if the query is not found on this node</li>
     *   <li>Silently ignores any errors during cancellation</li>
     *   <li>Still records cancellation metrics if the query is found</li>
     * </ul>
     *
     * <p><b>Usage in HA environments:</b> When a client requests cancellation but
     * the routing layer doesn't know which node is executing the query, it can
     * broadcast {@code tryCancel} to all nodes. Only the node actually running
     * the query will perform the cancellation; others will silently ignore the request.
     *
     * @param queryId the unique identifier of the query to cancel
     * @param context the call context containing peer identity
     */
    boolean tryCancel(Long queryId,
                   FlightProducer.CallContext context) throws SQLException;
}
