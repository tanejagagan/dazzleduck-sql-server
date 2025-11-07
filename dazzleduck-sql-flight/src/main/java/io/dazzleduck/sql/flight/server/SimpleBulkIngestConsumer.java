package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.vector.ipc.ArrowReader;

public interface SimpleBulkIngestConsumer extends FlightProducer {
    Runnable acceptPutStatementBulkIngest(
            FlightProducer.CallContext context,
            IngestionParameters ingestionParameters,
            ArrowReader inputReader,
            FlightProducer.StreamListener<PutResult> ackStream);

    String getProducerId();
    void cancel(Long queryId,
                FlightProducer.StreamListener<CancelStatus> listener,
                String peerIdentity);
}
