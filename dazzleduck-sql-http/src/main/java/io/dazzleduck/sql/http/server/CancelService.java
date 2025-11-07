package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.SimpleBulkIngestConsumer;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CancelService extends AbstractQueryBasedService {
    private static final Logger log = LoggerFactory.getLogger(CancelService.class);
    private final SimpleBulkIngestConsumer flightProducer;

    public CancelService(SimpleBulkIngestConsumer flightProducer, AccessMode accessMode) {
        super(accessMode);
        this.flightProducer = flightProducer;
    }

    @Override
    protected void handleInternal(ServerRequest request, ServerResponse response, QueryRequest requestObject) {
        var context = ControllerService.createContext(request);

        if (requestObject == null || requestObject.id() == null) {
            response.status(400).send(Map.of("error", "missing id in request"));
            return;
        }

        Long queryId = requestObject.id();
        try {
            flightProducer.cancel(queryId, new FlightProducer.StreamListener<CancelStatus>() {
                @Override
                public void onNext(CancelStatus val) {
                    log.info("Cancel update for id {}: {}", queryId, val);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Failed to cancel id {}: {}", queryId, t.getMessage(), t);
                    if (!response.isSent()) {
                        response.status(500).send("failed");
                    }
                }

                @Override
                public void onCompleted() {
                    log.info("Cancellation completed for id {} (status={})", queryId, CancelStatus.CANCELLED);
                    if (!response.isSent()) {
                        response.status(200).send("done");
                    }
                }
            }, context.peerIdentity());

        } catch (Exception ex) {
            log.error("Error while requesting cancel for id {}: {}", queryId, ex.getMessage(), ex);
            response.status(500).send(Map.of("error", "failed to request cancellation", "details", ex.getMessage()));
        }
    }
}
