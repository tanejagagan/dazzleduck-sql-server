package io.dazzleduck.sql.dataCollector;

import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import io.helidon.webserver.http.HttpRules;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataCollectorServer {

    public static void main(String[] args) {
        WebServer server = WebServer.builder()
                .port(8080)
                .routing(r -> r.register("/arrow", new ArrowUploadService()))
                .build()
                .start();

        System.out.println("Server running on http://localhost:8080/arrow");
    }

    static class ArrowUploadService implements HttpService {
        private final RootAllocator allocator = new RootAllocator();

        @Override
        public void routing(HttpRules rules) {
            rules.post(this::handleUpload);
        }

        private void handleUpload(ServerRequest req, ServerResponse res) {
            try (InputStream in = req.content().inputStream();
                 ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    int rowCount = root.getRowCount();
                    var vectors = root.getFieldVectors();
                    for (int i = 0; i < rowCount; i++) {
                        System.out.println("[METRIC ROW]");
                        for (var v : vectors) {
                            String columnName = v.getName();
                            Object value = v.getObject(i);
                            if ("tags".equals(columnName) && value instanceof List<?> list) {
                                value = list.stream()
                                        .map(o -> {
                                            var m = (Map<String, Object>) o;
                                            return m.get("key") + "=" + m.get("value");
                                        })
                                        .collect(Collectors.joining(", "));
                            }
                            System.out.printf("  %-8s : %s%n", columnName, value);
                        }
                        System.out.println("-----------------------------------------------------------");
                    }
                }


                res.send("OK");
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500).send("ERROR: " + e.getMessage());
            }
        }
    }
}
