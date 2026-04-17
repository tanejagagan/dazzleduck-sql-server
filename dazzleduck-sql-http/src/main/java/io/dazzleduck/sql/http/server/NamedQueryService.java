package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.flight.namedquery.NamedQueryRequest;
import io.dazzleduck.sql.flight.namedquery.NamedQueryServiceAdaptor;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HTTP service that executes named (templated) SQL queries.
 */
public class NamedQueryService implements HttpService, ControllerService {

    private static final Logger logger = LoggerFactory.getLogger(NamedQueryService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_PAGE_LIMIT = 20;
    private static final int MAX_PAGE_LIMIT = 200;

    private final NamedQueryServiceAdaptor adaptor;
    private final long timeoutMillis;

    public NamedQueryService(NamedQueryServiceAdaptor  adaptor, long timeoutMillis) {
        this.adaptor = adaptor;
        this.timeoutMillis = timeoutMillis;
    }


    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleList)
             .get("/{name}", this::handleGetByName)
             .post("/", this::handlePost);
    }

    /** GET / — returns a paginated list of named queries. */
    private void handleList(ServerRequest request, ServerResponse response) {
        try {
            long offset = ParameterUtils.getParameterValue("offset", request, 0L, Long.class);
            int limit  = ParameterUtils.getParameterValue("limit",  request, DEFAULT_PAGE_LIMIT, Integer.class);
            if (offset < 0) offset = 0;
            if (limit  < 1) limit  = DEFAULT_PAGE_LIMIT;
            if (limit  > MAX_PAGE_LIMIT) limit = MAX_PAGE_LIMIT;

            var callContext = ControllerService.createContext(request);
            var future = adaptor.listItemsDirect(offset, limit, callContext, () -> {
                response.headers().set(io.helidon.http.HeaderNames.CONTENT_TYPE, "application/json");
                return response.outputStream();
            });
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error listing named queries", e);
            if (!response.isSent()) {
                response.status(Status.INTERNAL_SERVER_ERROR_500).send("Failed to list named queries");
            }
        }
    }

    /** GET /{name} — returns the full named query object. */
    private void handleGetByName(ServerRequest request, ServerResponse response) {
        var segments = request.path().segments();
        String name = segments.get(segments.size() - 1).value();
        try {
            var callContext = ControllerService.createContext(request);
            var future = adaptor.getNamedQueryDirect(name, callContext, () -> {
                response.headers().set(io.helidon.http.HeaderNames.CONTENT_TYPE, "application/json");
                return response.outputStream();
            });
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof NamedQueryServiceAdaptor.TemplateNotFoundException) {
                logger.warn("Named query not found: {}", name);
                if (!response.isSent()) {
                    response.status(Status.NOT_FOUND_404).send(cause.getMessage());
                }
            } else {
                logger.error("Error fetching named query '{}'", name, cause);
                if (!response.isSent()) {
                    response.status(Status.INTERNAL_SERVER_ERROR_500).send("Failed to fetch named query");
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching named query '{}'", name, e);
            if (!response.isSent()) {
                response.status(Status.INTERNAL_SERVER_ERROR_500).send("Failed to fetch named query");
            }
        }
    }

    private void handlePost(ServerRequest request, ServerResponse response) {
        try {
            var namedQuery = MAPPER.readValue(request.content().inputStream(), NamedQueryRequest.class);

            if (namedQuery.name() == null || namedQuery.name().isBlank()) {
                response.status(Status.BAD_REQUEST_400).send("Missing required field: name");
                return;
            }

            var callContext = ControllerService.createContext(request);
            var compressionCodec = ParameterUtils.getArrowCompression(request);
            adaptor.getStreamNamedQueryDirect(namedQuery.name(), namedQuery.parameters(),
                    callContext, () -> response.outputStream(), compressionCodec)
                .get(timeoutMillis, TimeUnit.MILLISECONDS);

        } catch (TimeoutException e) {
            logger.error("Named query execution timed out after {}ms", timeoutMillis);
            if (!response.isSent()) {
                response.status(Status.GATEWAY_TIMEOUT_504).send("Query execution timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Named query execution interrupted", e);
            if (!response.isSent()) {
                response.status(Status.INTERNAL_SERVER_ERROR_500).send("Query execution interrupted");
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof ParameterValidationException) {
                logger.warn("Parameter validation failed for named query: {}", cause.getMessage());
                if (!response.isSent()) {
                    response.status(Status.BAD_REQUEST_400).send(cause.getMessage());
                }
            } else if (cause instanceof NamedQueryServiceAdaptor.TemplateNotFoundException) {
                logger.warn("Named query not found: {}", cause.getMessage());
                if (!response.isSent()) {
                    response.status(Status.NOT_FOUND_404).send(cause.getMessage());
                }
            } else {
                logger.error("Error executing named query", cause);
                if (!response.isSent()) {
                    handleFlightError(response, cause);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing named query", e);
            if (!response.isSent()) {
                handleFlightError(response, e);
            }
        }
    }

    private void handleFlightError(ServerResponse response, Throwable cause) {
        String errorMsg = cause.getMessage() != null ? cause.getMessage() : "Internal server error";
        Status httpStatus = Status.INTERNAL_SERVER_ERROR_500;

        if (cause instanceof FlightRuntimeException flightEx) {
            FlightStatusCode code = flightEx.status().code();
            if (flightEx.status().description() != null) {
                errorMsg = flightEx.status().description();
            }
            httpStatus = switch (code) {
                case INVALID_ARGUMENT -> Status.BAD_REQUEST_400;
                case UNAUTHENTICATED -> Status.UNAUTHORIZED_401;
                case UNAUTHORIZED -> Status.FORBIDDEN_403;
                case NOT_FOUND -> Status.NOT_FOUND_404;
                case TIMED_OUT -> Status.GATEWAY_TIMEOUT_504;
                case UNIMPLEMENTED -> Status.NOT_IMPLEMENTED_501;
                case UNAVAILABLE -> Status.SERVICE_UNAVAILABLE_503;
                default -> Status.INTERNAL_SERVER_ERROR_500;
            };
        }

        response.status(httpStatus).send(errorMsg);
    }
}
