package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.hubspot.jinjava.Jinjava;
import io.dazzleduck.sql.common.NamedQueryParameterValidator;
import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.http.server.model.HttpConfig;
import io.dazzleduck.sql.http.server.model.NamedQueryListItem;
import io.dazzleduck.sql.http.server.model.NamedQueryPage;
import io.dazzleduck.sql.http.server.model.NamedQueryRequest;
import io.dazzleduck.sql.http.server.model.NamedQueryResponse;
import io.dazzleduck.sql.http.server.model.NamedQueryTemplate;
import io.dazzleduck.sql.http.server.model.QueryMode;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HTTP service that executes named (templated) SQL queries.
 *
 * <p>Accepts a POST with a {@link NamedQueryRequest} body (name + parameters). The service:
 * <ol>
 *   <li>Fetches the {@link NamedQueryTemplate} from the DB by primary key.</li>
 *   <li>Resolves each {@link NamedQueryParameterValidator} class name using a shared validator
 *       instance cache to avoid repeated reflection.</li>
 *   <li>Runs all validators and collects every failure before returning HTTP 400.</li>
 *   <li>Renders the SQL template with Jinjava (Jinja2 syntax) using the supplied parameters.</li>
 *   <li>Executes the rendered SQL and streams the Arrow result back to the caller.</li>
 * </ol>
 *
 * <p>Expected DB table schema:
 * <pre>{@code
 *   CREATE TABLE named_queries (
 *       name                   VARCHAR PRIMARY KEY,
 *       template               VARCHAR,
 *       validators             VARCHAR[],
 *       description            VARCHAR,
 *       parameter_descriptions MAP(VARCHAR, VARCHAR)
 *   );
 * }</pre>
 */
public class NamedQueryService implements HttpService, ControllerService {

    private static final Logger logger = LoggerFactory.getLogger(NamedQueryService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_PAGE_LIMIT = 20;
    private static final int MAX_PAGE_LIMIT = 200;
    private static final int MAX_VALIDATOR_CACHE_SIZE = 500;

    private final HttpFlightAdaptor httpFlightAdaptor;
    private final HttpConfig httpConfig;
    private final String namedQueryTable;
    private final Jinjava jinjava;

    /** Validator instance cache: fully-qualified class name → singleton instance. */
    private final ConcurrentHashMap<String, NamedQueryParameterValidator> validatorCache = new ConcurrentHashMap<>();

    public NamedQueryService(HttpFlightAdaptor httpFlightAdaptor, String namedQueryTable) {
        this(httpFlightAdaptor, namedQueryTable, HttpConfig.defaultConfig());
    }

    public NamedQueryService(HttpFlightAdaptor httpFlightAdaptor, String namedQueryTable,
                             HttpConfig httpConfig) {
        this.httpFlightAdaptor = httpFlightAdaptor;
        this.namedQueryTable = namedQueryTable;
        this.httpConfig = httpConfig;
        this.jinjava = new Jinjava();
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleList)
             .get("/{name}", this::handleGetByName)
             .post("/", this::handlePost);
    }

    /** GET / — returns a paginated list of named queries (name + description only). */
    private void handleList(ServerRequest request, ServerResponse response) {
        try {
            int offset = ParameterUtils.getParameterValue("offset", request, 0, Integer.class);
            int limit  = ParameterUtils.getParameterValue("limit",  request, DEFAULT_PAGE_LIMIT, Integer.class);
            if (offset < 0) offset = 0;
            if (limit  < 1) limit  = DEFAULT_PAGE_LIMIT;
            if (limit  > MAX_PAGE_LIMIT) limit = MAX_PAGE_LIMIT;

            NamedQueryPage.WithTotal page = loadPageFromDb(offset, limit);
            long total = page.total();
            List<NamedQueryListItem> items = page.items();

            response.headers().set(io.helidon.http.HeaderNames.CONTENT_TYPE, "application/json");
            response.send(MAPPER.writeValueAsBytes(new NamedQueryPage(items, total, offset, limit)));
        } catch (Exception e) {
            logger.error("Error listing named queries", e);
            response.status(Status.INTERNAL_SERVER_ERROR_500).send("Failed to list named queries");
        }
    }

    /** GET /{name} — returns the full named query object including validator descriptions. */
    private void handleGetByName(ServerRequest request, ServerResponse response) {
        var segments = request.path().segments();
        String name = segments.get(segments.size() - 1).value();
        try {
            NamedQueryTemplate template = loadFromDb(name);
            response.headers().set(io.helidon.http.HeaderNames.CONTENT_TYPE, "application/json");
            response.send(MAPPER.writeValueAsBytes(toInfo(template)));
        } catch (TemplateNotFoundException e) {
            logger.warn("Named query not found: {}", name);
            response.status(Status.NOT_FOUND_404).send(e.getMessage());
        } catch (Exception e) {
            logger.error("Error fetching named query '{}'", name, e);
            response.status(Status.INTERNAL_SERVER_ERROR_500).send("Failed to fetch named query");
        }
    }

    private void handlePost(ServerRequest request, ServerResponse response) {
        try {
            var namedQuery = MAPPER.readValue(request.content().inputStream(), NamedQueryRequest.class);

            if (namedQuery.name() == null || namedQuery.name().isBlank()) {
                response.status(Status.BAD_REQUEST_400).send("Missing required field: name");
                return;
            }

            NamedQueryTemplate queryTemplate = loadFromDb(namedQuery.name());

            runValidators(queryTemplate, namedQuery.parameters());

            Map<String, Object> jinjavaContext = new HashMap<>(
                    namedQuery.parameters() != null ? namedQuery.parameters() : Map.of());
            String sql = jinjava.render(queryTemplate.template(), jinjavaContext);
            sql = applyMode(sql, namedQuery.mode());
            logger.debug("Rendered SQL for named query '{}' (mode={}): {}", namedQuery.name(), namedQuery.mode(), sql);

            var callContext = ControllerService.createContext(request);
            var id = StatementHandle.nextStatementId();
            var statementHandle = StatementHandle.newStatementHandle(
                    id, sql, httpFlightAdaptor.getProducerId(), -1);
            var ticket = FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)))
                    .build();

            var compressionCodec = ParameterUtils.getArrowCompression(request);
            var future = httpFlightAdaptor.getStreamStatementDirect(
                    ticket, callContext, response::outputStream, compressionCodec);
            future.get(httpConfig.getQueryTimeoutMs(), TimeUnit.MILLISECONDS);

        } catch (ParameterValidationException e) {
            logger.warn("Parameter validation failed for named query: {}", e.getMessage());
            if (!response.isSent()) {
                response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            }
        } catch (TemplateNotFoundException e) {
            logger.warn("Named query not found: {}", e.getMessage());
            if (!response.isSent()) {
                response.status(Status.NOT_FOUND_404).send(e.getMessage());
            }
        } catch (TimeoutException e) {
            logger.error("Named query execution timed out after {}ms", httpConfig.getQueryTimeoutMs());
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
            logger.error("Error executing named query", cause);
            if (!response.isSent()) {
                handleFlightError(response, cause);
            }
        } catch (Exception e) {
            logger.error("Error processing named query", e);
            if (!response.isSent()) {
                handleFlightError(response, e);
            }
        }
    }

    private String applyMode(String sql, QueryMode mode) {
        return switch (mode) {
            case EXPLAIN         -> "EXPLAIN " + sql;
            case EXPLAIN_ANALYZE -> "EXPLAIN ANALYZE " + sql;
            case EXECUTE         -> sql;
        };
    }

    /**
     * Runs every {@link NamedQueryParameterValidator} listed in the template, collecting all
     * failures. A single {@link ParameterValidationException} is thrown with all messages
     * joined by {@code "; "}.
     */
    private void runValidators(NamedQueryTemplate queryTemplate, Map<String, String> parameters)
            throws ParameterValidationException {
        String[] validatorClassNames = queryTemplate.validators();
        if (validatorClassNames == null || validatorClassNames.length == 0) {
            return;
        }
        Map<String, String> safeParams = parameters != null ? parameters : Map.of();
        List<String> errors = new ArrayList<>();
        for (String className : validatorClassNames) {
            try {
                getOrCreateValidator(className).validate(safeParams);
            } catch (ParameterValidationException e) {
                errors.add(e.getMessage());
            } catch (RuntimeException e) {
                errors.add("Validator configuration error: " + className + " — " + e.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            throw new ParameterValidationException(String.join("; ", errors));
        }
    }

    /**
     * Returns a cached {@link NamedQueryParameterValidator}, instantiating via reflection on
     * first use. Capped at {@link #MAX_VALIDATOR_CACHE_SIZE} entries.
     */
    private NamedQueryParameterValidator getOrCreateValidator(String className) {
        NamedQueryParameterValidator cached = validatorCache.get(className);
        if (cached != null) {
            return cached;
        }
        NamedQueryParameterValidator instance = instantiateValidator(className);
        if (validatorCache.size() < MAX_VALIDATOR_CACHE_SIZE) {
            NamedQueryParameterValidator existing = validatorCache.putIfAbsent(className, instance);
            return existing != null ? existing : instance;
        }
        return instance;
    }

    private NamedQueryParameterValidator instantiateValidator(String className) {
        try {
            return (NamedQueryParameterValidator) Class.forName(className)
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate validator: " + className, e);
        }
    }

    /**
     * Fetches a single row from the named-query table by primary key.
     *
     * @throws TemplateNotFoundException if no row exists for the given name
     */
    private NamedQueryTemplate loadFromDb(String name) throws SQLException, TemplateNotFoundException {
        String safeName = name.replace("'", "''");
        String sql = ("SELECT name, template, validators, description, parameter_descriptions" +
                      " FROM %s WHERE name = '%s'")
                .formatted(namedQueryTable, safeName);
        try (DuckDBConnection connection = ConnectionPool.getConnection()) {
            var iter = ConnectionPool.collectAll(connection, sql, NamedQueryTemplate.class).iterator();
            if (!iter.hasNext()) {
                throw new TemplateNotFoundException("Named query not found: " + name);
            }
            return iter.next();
        }
    }

    /**
     * Converts a {@link NamedQueryTemplate} to a {@link NamedQueryResponse}, replacing validator
     * class names with their {@link NamedQueryParameterValidator#description()} values.
     */
    private NamedQueryResponse toInfo(NamedQueryTemplate t) {
        List<String> validatorDescriptions;
        if (t.validators() == null || t.validators().length == 0) {
            validatorDescriptions = Collections.emptyList();
        } else {
            validatorDescriptions = new ArrayList<>(t.validators().length);
            for (String className : t.validators()) {
                try {
                    String desc = getOrCreateValidator(className).description();
                    validatorDescriptions.add(desc.isBlank() ? className : desc);
                } catch (Exception e) {
                    logger.warn("Could not load validator '{}' for description: {}", className, e.getMessage());
                    validatorDescriptions.add(className);
                }
            }
        }
        return new NamedQueryResponse(t.name(), t.description(), t.parameterDescriptions(), validatorDescriptions);
    }

    /**
     * Fetches a page of name+description pairs and the total row count in one query
     * using a {@code COUNT(*) OVER ()} window function.
     */
    private NamedQueryPage.WithTotal loadPageFromDb(int offset, int limit) throws SQLException {
        String sql = ("SELECT name, description, COUNT(*) OVER () FROM %s ORDER BY name LIMIT %d OFFSET %d")
                .formatted(namedQueryTable, limit, offset);
        try (DuckDBConnection connection = ConnectionPool.getConnection()) {
            List<NamedQueryListItem> items = new ArrayList<>();
            long[] total = {0L};
            for (NamedQueryListItem item : ConnectionPool.collectAll(connection, sql, rs -> {
                total[0] = rs.getLong(3);
                return new NamedQueryListItem(rs.getString(1), rs.getString(2));
            })) {
                items.add(item);
            }
            return new NamedQueryPage.WithTotal(items, total[0]);
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

    /** Signals that a named query template could not be found in the DB. */
    static class TemplateNotFoundException extends Exception {
        TemplateNotFoundException(String message) {
            super(message);
        }
    }
}
