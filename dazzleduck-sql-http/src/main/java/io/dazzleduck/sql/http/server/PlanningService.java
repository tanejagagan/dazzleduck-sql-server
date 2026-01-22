package io.dazzleduck.sql.http.server;

import com.google.protobuf.Any;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.http.server.model.Descriptor;
import io.dazzleduck.sql.http.server.model.PlanResponse;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.util.ArrayList;

public class PlanningService extends AbstractQueryBasedService implements ParameterUtils {

    private final FlightProducer flightProducer;
    private final FlightToHttpEndpointMapper endpointMapper;

    public PlanningService(FlightProducer flightProducer, FlightToHttpEndpointMapper endpointMapper) {
        this.flightProducer = flightProducer;
        this.endpointMapper = endpointMapper;
    }

    @Override
    protected void handleInternal(ServerRequest request, ServerResponse response, QueryRequest queryRequest) {
        var context = ControllerService.createContext(request);

        try {
            var command = FlightSql.CommandStatementQuery.newBuilder()
                    .setQuery(queryRequest.query())
                    .build();
            var info = flightProducer.getFlightInfo(context,
                    FlightDescriptor.command(Any.pack(command).toByteArray()));

            var result = new ArrayList<PlanResponse>();
            for (var endpoint : info.getEndpoints()) {
                var any = FlightSqlUtils.parseOrThrow(endpoint.getTicket().getBytes());
                var statementQuery = FlightSqlUtils.unpackOrThrow(any, FlightSql.TicketStatementQuery.class);
                var statementHandle = MAPPER.readValue(statementQuery.getStatementHandle().toByteArray(), StatementHandle.class);
                var locations = endpoint.getLocations().stream()
                        .map(loc -> endpointMapper.getHttpEndpoint(loc.getUri().toString()))
                        .toList();
                var descriptor = new Descriptor(statementHandle);
                result.add(new PlanResponse(locations, descriptor));
            }

            // Proper resource management with try-with-resources
            try (var outputStream = response.outputStream()) {
                MAPPER.writeValue(outputStream, result);
            }

        } catch (IOException e) {
            logger.atError().setCause(e).log("IO error during query planning");
            String errorMsg = e.getMessage() != null ? e.getMessage() : "IO error during planning";
            response.status(io.helidon.http.Status.INTERNAL_SERVER_ERROR_500);
            response.send(errorMsg);
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error during query planning");
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Internal server error";
            response.status(io.helidon.http.Status.INTERNAL_SERVER_ERROR_500);
            response.send(errorMsg);
        }
    }
}

