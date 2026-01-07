package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.PutResult;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils, ControllerService {

    private final HttpFlightAdaptor httpFlightAdaptor;

    public IngestionService(HttpFlightAdaptor httpFlightAdaptor)  {
        this.httpFlightAdaptor = httpFlightAdaptor;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    protected boolean handleMismatchContentType(ServerRequest serverRequest, ServerResponse serverResponse){
        var contentType = serverRequest.headers().value(HeaderNames.CONTENT_TYPE);
        if (contentType.isEmpty() || !contentType.get().equals(ContentTypes.APPLICATION_ARROW)) {
            serverResponse.status(Status.UNSUPPORTED_MEDIA_TYPE_415);
            serverResponse.send();
            return true;
        }
        return false;
    }

    protected IngestionParameters parseIngestionParameters(ServerRequest serverRequest) {
        UriQuery query = serverRequest.query();
        var path = query.get("path");

        // Validate path to prevent path traversal attacks
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path parameter is required");
        }
        if (path.contains("..") || path.startsWith("/")) {
            throw new IllegalArgumentException("Invalid path: path traversal not allowed");
        }


        String format = ParameterUtils.getParameterValue(HEADER_DATA_FORMAT, serverRequest, "parquet", String.class);
        var partitionString = urlDecode(
                ParameterUtils.getParameterValue(HEADER_DATA_PARTITION, serverRequest, null, String.class));
        var tranformationString = urlDecode(
                ParameterUtils.getParameterValue(HEADER_DATA_TRANSFORMATION, serverRequest, null, String.class));
        var producerId = ParameterUtils.getParameterValue(HEADER_PRODUCER_ID, serverRequest, null, String.class);
        var producerBatchId = ParameterUtils.getParameterValue(HEADER_PRODUCER_BATCH_ID, serverRequest, -1L, Long.class);
        var sortOrderString = urlDecode(
                ParameterUtils.getParameterValue(HEADER_SORT_ORDER, serverRequest, null, String.class));
        return new IngestionParameters(path, format, getArray(partitionString),
                getArray(tranformationString), getArray(sortOrderString), producerId, producerBatchId, Map.of());
    }

    private String[] getArray(String stringValue) {
        return stringValue == null? new String[0]: stringValue.split(",");
    }

    private String urlDecode(String string){
        if (string == null) {
            return null;
        }
        else {
            return URLDecoder.decode(string, StandardCharsets.UTF_8);
        }
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) {
        if (handleMismatchContentType(serverRequest, serverResponse)) {
            return;
        }

        try {
            var context = ControllerService.createContext(serverRequest);
            var ingestionParameters = parseIngestionParameters(serverRequest);
            InputStream inputStream = serverRequest.content().inputStream();

            // Track if response has been sent to prevent double-send
            final boolean[] responseSent = {false};

            var runnable = httpFlightAdaptor.acceptPutStatementBulkIngest(context, ingestionParameters,
                    inputStream, new FlightClient.PutListener() {

                        @Override
                        public void getResult() {

                        }

                        @Override
                        public void onNext(PutResult val) {

                        }

                        @Override
                        synchronized public void onError(Throwable t) {
                            if (!responseSent[0]) {
                                responseSent[0] = true;
                                String errorMsg = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
                                serverResponse.status(Status.BAD_REQUEST_400);
                                serverResponse.send(errorMsg.getBytes());
                            }
                        }

                        @Override
                        synchronized public void onCompleted() {
                            if (!responseSent[0]) {
                                responseSent[0] = true;
                                serverResponse.status(Status.OK_200);
                                serverResponse.send();
                            }
                        }
                    });
            runnable.run();

        } catch (IllegalArgumentException e) {
            // Path validation or parameter errors
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Invalid request parameters";
            serverResponse.status(Status.BAD_REQUEST_400);
            serverResponse.send(errorMsg);
        } catch (Exception e) {
            // Catch all other exceptions to prevent throwing from HTTP handler
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Internal server error";
            serverResponse.status(Status.INTERNAL_SERVER_ERROR_500);
            serverResponse.send(errorMsg);
        }
    }
}
