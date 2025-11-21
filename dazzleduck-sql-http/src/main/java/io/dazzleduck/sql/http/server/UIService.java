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

        var response  = """
                <table>
                  <caption>
                    Monthly Sales Report
                  </caption>
                  <thead>
                    <tr>
                      <th>Month</th>
                      <th>Sales</th>
                      <th>Profit</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>January</td>
                      <td>$10,000</td>
                      <td>$2,000</td>
                    </tr>
                    <tr>
                      <td>February</td>
                      <td>$12,000</td>
                      <td>$2,500</td>
                    </tr>
                  </tbody>
                  <tfoot>
                    <tr>
                      <td>Total</td>
                      <td>$22,000</td>
                      <td>$4,500</td>
                    </tr>
                  </tfoot>
                </table>
                """;
        serverResponse.send(response.getBytes());
    }
}