package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.hadoop.shaded.org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CancelService extends AbstractQueryBasedService {
    private static final Logger log = LoggerFactory.getLogger(CancelService.class);
    private final HttpFlightAdaptor httpFlightAdaptor;

    public CancelService(HttpFlightAdaptor httpFlightAdaptor) {
        this.httpFlightAdaptor = httpFlightAdaptor;
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
            var res = httpFlightAdaptor.tryCancel(queryId, context);
            if (res) {
                response.status(HttpStatus.SC_OK).send("query cancelled successfully");
            } else {
                response.status(HttpStatus.SC_NOT_FOUND).send("query not found");
            }
        } catch (Exception ex) {
            log.error("Error while requesting cancel for id {}: {}", queryId, ex.getMessage(), ex);
            response.status(Status.INTERNAL_SERVER_ERROR_500).send("Error while requesting cancel: " + ex.getMessage());
        }
    }
}
