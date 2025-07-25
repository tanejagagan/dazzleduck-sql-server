package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class QueryService extends AbstractQueryBasedService {

    private final BufferAllocator allocator;

    public QueryService(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    protected void handleInternal(ServerRequest request,
                                ServerResponse response, String query) {

        var fetchSizeHeader = request.headers().value(HeaderNames.create(Headers.HEADER_FETCH_SIZE));
        int fetchSize = fetchSizeHeader.map(Integer::parseInt).orElse(Headers.DEFAULT_ARROW_FETCH_SIZE);
        String schema = request.headers().value(HeaderNames.create(Headers.HEADER_DATA_SCHEMA)).orElse(null);
        try (var connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, getOverrideQuery(schema, query), fetchSize);
             var vsr = reader.getVectorSchemaRoot();
             var os = response.outputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, Channels.newChannel(os))) {
            var respHeaders = response.headers();
            respHeaders.set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_ARROW);
            response.status(Status.OK_200);
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
        } catch (SQLException e) {
            throw new BadRequestException(400, e.getMessage());
        } catch (Exception e) {
            throw new InternalErrorException(500, e.getMessage());
        }
    }

    private static String getOverrideQuery(String schema, String query) throws SQLException, JsonProcessingException {
        if (schema == null || schema.isBlank()) return query;
        String decoded = URLDecoder.decode(schema, StandardCharsets.UTF_8);
        String cast = Transformations.getCast(decoded);
        String finalQuery = "select " + cast + " where false union all " + query + ";";
        System.out.println("Final Query Will be: -> " + finalQuery);
        return finalQuery;
    }
}
