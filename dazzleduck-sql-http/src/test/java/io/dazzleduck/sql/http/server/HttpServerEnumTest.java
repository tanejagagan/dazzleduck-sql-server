package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ContentTypes;
import io.dazzleduck.sql.commons.io.ResultStreams;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DuckDB sends ENUM columns dictionary-encoded. /v1/query must resolve them to their values in every
 * output format: each query here returns the same as the same query with VARCHAR in place of the ENUM.
 */
public class HttpServerEnumTest extends HttpServerTestBase {

    private static final String ENUM = "ENUM('sad', 'ok', 'happy')";

    /** ENUM in every position a dictionary can sit; {@code %s} is the column type. */
    private static final String QUERY = """
            SELECT 'ok'::%1$s AS top, NULL::%1$s AS null_value,
                   ['happy'::%1$s, NULL, 'sad'::%1$s] AS list,
                   {'mood': 'sad'::%1$s, 'missing': NULL::%1$s, 'n': 1} AS struct,
                   MAP {'k': 'ok'::%1$s} AS map,
                   [{'inner': ['ok'::%1$s]}] AS deep,
                   ['sad'::%1$s, 'ok'::%1$s]::%1$s[2] AS fixed
            FROM range(3)
            """;

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
        installArrowExtension();
    }

    @AfterAll
    static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    private HttpResponse<byte[]> query(String sql, String accept) throws Exception {
        var uri = URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
        var builder = authenticatedRequestBuilder(uri).GET();
        if (accept != null) {
            builder.header("Accept", accept);
        }
        var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return response;
    }

    private String text(String sql, String accept) throws Exception {
        return new String(query(sql, accept).body(), StandardCharsets.UTF_8);
    }

    /** The Arrow response, read back and written as TSV with the stream's own dictionaries. */
    private String arrowAsTsv(String sql) throws Exception {
        var out = new ByteArrayOutputStream();
        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(new java.io.ByteArrayInputStream(query(sql, null).body()),
                     allocator, CommonsCompressionFactory.INSTANCE)) {
            ResultStreams.writeTsv(reader, out);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void enumMatchesVarcharAsArrow() throws Exception {
        assertEquals(arrowAsTsv(QUERY.formatted("VARCHAR")), arrowAsTsv(QUERY.formatted(ENUM)));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void enumMatchesVarcharAsTsv() throws Exception {
        assertEquals(text(QUERY.formatted("VARCHAR"), ContentTypes.TEXT_TSV),
                text(QUERY.formatted(ENUM), ContentTypes.TEXT_TSV));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void enumMatchesVarcharAsJsonl() throws Exception {
        assertEquals(text(QUERY.formatted("VARCHAR"), ContentTypes.APPLICATION_JSONL),
                text(QUERY.formatted(ENUM), ContentTypes.APPLICATION_JSONL));
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void enumInsideAUnionIsReportedAsAnErrorInTsvAndJsonl() throws Exception {
        // Not resolvable there: both formats must fail with the reason, before any row is written,
        // rather than print dictionary indices or an empty 200.
        var uri = URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(
                "SELECT union_value(e := 'ok'::" + ENUM + ") AS u FROM range(3)", StandardCharsets.UTF_8));
        for (String accept : new String[]{ContentTypes.TEXT_TSV, ContentTypes.APPLICATION_JSONL}) {
            var response = client.send(authenticatedRequestBuilder(uri).GET().header("Accept", accept).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(500, response.statusCode(), accept + ": " + response.body());
            assertTrue(response.body().contains("Column 'u' has a dictionary-encoded value inside Union"),
                    accept + ": " + response.body());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void enumPrintsItsValues() throws Exception {
        // Guards the comparisons above against both sides being wrong the same way.
        String jsonl = text(QUERY.formatted(ENUM), ContentTypes.APPLICATION_JSONL).lines().findFirst().orElseThrow();
        assertTrue(jsonl.startsWith("{\"top\":\"ok\",\"null_value\":null,\"list\":[\"happy\",null,\"sad\"],"
                + "\"struct\":{\"mood\":\"sad\",\"missing\":null,\"n\":1},\"map\":{\"k\":\"ok\"},"
                + "\"deep\":[{\"inner\":[\"ok\"]}],\"fixed\":[\"sad\",\"ok\"]}"), jsonl);
        String tsvRow = text(QUERY.formatted(ENUM), ContentTypes.TEXT_TSV).lines().skip(1).findFirst().orElseThrow();
        assertTrue(tsvRow.startsWith("ok\t\t[\"happy\",null,\"sad\"]\t"), tsvRow);
    }
}
