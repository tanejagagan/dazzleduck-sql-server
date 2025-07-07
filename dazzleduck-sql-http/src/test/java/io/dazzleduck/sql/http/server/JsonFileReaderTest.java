package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class JsonFileReaderTest {

    @Test
    public void test() throws JsonProcessingException {
        ObjectMapper mapper  = new ObjectMapper();
        String json = "{\"key\" : 99, \"obj\" : {\"number\" : 10}, \"array\" : [1, 2, 3 ]}";

        try(var bufferAllocator = new RootAllocator()) {
            var tree = (ObjectNode) mapper.readTree(json);
            buildSchemaForObject(tree);

        }
    }

    public static void buildSchema(String name, JsonNode node) {
        switch (node) {
            case IntNode i -> {
                System.out.println(i.toString());
            }
            case ObjectNode o -> {
                var it = o.fields();
                while (it.hasNext()) {
                    var m = it.next();
                    buildSchema(m.getKey(), m.getValue());
                }
            }
            case ArrayNode a -> {
                var firstNode = a.get(0);
                switch (firstNode) {
                    case ObjectNode objectNode -> buildSchemaForObject(objectNode);
                    case IntNode n -> {
                        System.out.println(n);
                    }
                    case LongNode n -> {
                        System.out.println(n);
                    }

                    case TextNode n -> {
                        System.out.println(n);
                    }
                    case BooleanNode n -> {
                        System.out.println(n);
                    }

                    default -> throw new IllegalStateException("Unexpected value: " + firstNode);
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + node);
        }
    }

    public static void buildSchemaForObject(ObjectNode tree) {
        var it = tree.fields();
        while (it.hasNext()) {
            var e = it.next();
            buildSchema(e.getKey(), e.getValue());
        }
    }

    public static void buildSchemaForSimpleType(JsonNode node) {

    }
}
