package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

public class UIService implements HttpService {
    DuckDBFlightSqlProducer producer;
    public UIService(DuckDBFlightSqlProducer producer) {
        this.producer = producer;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleGet);
    }

    private void handleGet(ServerRequest serverRequest, ServerResponse serverResponse) {
        serverResponse.send("In Progress".getBytes());
    }
}