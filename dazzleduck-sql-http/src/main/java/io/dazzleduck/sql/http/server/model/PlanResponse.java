package io.dazzleduck.sql.http.server.model;

import java.util.List;

public record PlanResponse(List<String> endpoints,  Descriptor descriptor) {
}
