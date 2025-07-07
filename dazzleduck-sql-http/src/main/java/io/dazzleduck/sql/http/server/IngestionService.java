package io.dazzleduck.sql.http.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class IngestionService implements HttpService {
    private final String warehousePath;
    private final BufferAllocator allocator;

    public IngestionService(String warehousePath, BufferAllocator allocator) {
        this.warehousePath = warehousePath;
        this.allocator = allocator;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws SQLException {
        UriQuery query = serverRequest.query();
        var path = query.get("path");
        final String completePath = warehousePath + "/" + path;
        String format = query.contains("format") ? query.get("format") : "parquet";

        List<String> partitionColumns;
        if (query.contains("partition")) {
            partitionColumns = Arrays.stream(query.get("partition").split(",")).toList();
        } else {
            partitionColumns = List.of();
        }
        var reader = new ArrowStreamReader(serverRequest.content().inputStream(), allocator);
        ConnectionPool.bulkIngestToFile(reader, allocator, completePath, partitionColumns, format);
        serverResponse.status(Status.OK_200);
        serverResponse.send();
    }
}
