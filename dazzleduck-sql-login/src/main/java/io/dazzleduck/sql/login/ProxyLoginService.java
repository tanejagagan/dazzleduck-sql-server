package io.dazzleduck.sql.login;

import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ProxyLoginService implements HttpService {
    // API versioning
    public static final String API_VERSION = "v1";
    public static final String API_VERSION_PREFIX = "/" + API_VERSION;

    // Endpoints
    public static final String ENDPOINT_LOGIN = "/";

    // HTTP headers
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final HttpClient client = HttpClient.newHttpClient();
    private final String target;
    private static final Logger logger = LoggerFactory.getLogger(ProxyLoginService.class);
    public ProxyLoginService(String target) {
        this.target = target;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(ENDPOINT_LOGIN, this::forwardLogin);
    }

    private void forwardLogin(ServerRequest req, ServerResponse res) throws IOException {
        try {
            logger.debug("Forwarding login request to: {}", target);
            String requestJson = req.content().as(String.class);
            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(URI.create(target))
                    .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(requestJson))
                    .build();

            HttpResponse<String> backendResponse = client.send(httpReq, HttpResponse.BodyHandlers.ofString());
            res.status(Status.create(backendResponse.statusCode()));
            res.send(backendResponse.body());
            logger.info("Login forwarded successfully with status: {}", backendResponse.statusCode());
        } catch (Exception e) {
            logger.error("Login forwarding failed: {}", e.getMessage(), e);
            res.status(Status.INTERNAL_SERVER_ERROR_500).send("Login forwarding failed: " + e.getMessage());
        }
    }
}
