package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.databind.JsonNode;

public record TreeAndSize (JsonNode tree, long size) {
}
