package io.dazzleduck.sql.http.server;

public interface FlightToHttpEndpointMapper {
    String getHttpEndpoint(String flightEndpoint);

    static FlightToHttpEndpointMapper fixed(String httpEndpoint) {
        return new FlightToHttpEndpointMapper() {
            @Override
            public String getHttpEndpoint(String flightEndpoint) {
                 return httpEndpoint;
            }
        };
    }
}
