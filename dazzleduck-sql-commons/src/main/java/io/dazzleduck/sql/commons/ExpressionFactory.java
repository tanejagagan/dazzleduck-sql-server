package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static io.dazzleduck.sql.commons.ExpressionConstants.FUNCTION_CLASS;
import static io.dazzleduck.sql.commons.ExpressionConstants.FUNCTION_TYPE;

public class ExpressionFactory {



    public static JsonNode reference(String[] value) {
        ObjectNode result = withClassType(ExpressionConstants.COLUMN_REF_CLASS, ExpressionConstants.COLUMN_REF_TYPE);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        for (String string : value) {
            arrayNode.add(string);
        }
        result.set("column_names", arrayNode);
        return result;
    }

    public static JsonNode equalExpr(JsonNode left, JsonNode right) {
        return comparing(ExpressionConstants.COMPARE_TYPE_EQUAL, left, right);
    }

    public static JsonNode lessThanOrEqualExpr(JsonNode left, JsonNode right) {
        return comparing(ExpressionConstants.COMPARE_TYPE_LESSTHANOREQUALTO, left, right);
    }

    public static JsonNode greaterThanOrEqualExpr(JsonNode left, JsonNode right) {
        return comparing(ExpressionConstants.COMPARE_TYPE_GREATERTHANOREQUALTO, left, right);
    }

    public static JsonNode cast(JsonNode child, String castType) {
        ObjectNode result = withClassType(ExpressionConstants.CAST_CLASS, ExpressionConstants.CAST_TYPE_OPERATOR);
        result.set("child", child);
        ObjectNode ct = new ObjectNode(JsonNodeFactory.instance);
        ct.put("id", castType);
        ct.set("type_info", null);
        result.set("cast_type", ct);
        result.put("try_cast", false);
        return result;
    }

    public static JsonNode caseCheck(JsonNode when, JsonNode then) {
        ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        result.set("when_expr", when);
        result.set("then_expr", then);
        return result;
    }

    public static JsonNode ifExpr(JsonNode condition,
                                  JsonNode then,
                                  JsonNode elseExpression) {
        ObjectNode result = withClassType(ExpressionConstants.CASE_CLASS, ExpressionConstants.CASE_TYPE_EXPR);
        ArrayNode caseChecks = new ArrayNode(JsonNodeFactory.instance);
        caseChecks.add(caseCheck(condition, then));
        result.set("case_checks", caseChecks);
        result.set("else_expr", elseExpression);
        return result;
    }

    public static JsonNode trueExpression() {
        return ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
    }

    public static JsonNode falseExpression() {
        return ExpressionFactory.cast(ExpressionFactory.constant("f"), "BOOLEAN");
    }

    public static JsonNode constant(Object value) {
        ObjectNode result = withClassType(ExpressionConstants.CONSTANT_CLASS, ExpressionConstants.CONSTANT_TYPE);
        JsonNode valueNode = constantValueNode(value);
        result.set("value", valueNode);
        return result;
    }

    public static JsonNode andFilters(JsonNode leftFilter, JsonNode rightFilter) {
        ObjectNode result = withClassType(ExpressionConstants.CONJUNCTION_CLASS, ExpressionConstants.CONJUNCTION_TYPE_AND);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        arrayNode.add(leftFilter);
        arrayNode.add(rightFilter);
        result.set("children", arrayNode);
        return result;
    }

    public static JsonNode orFilters(JsonNode leftFilter, JsonNode rightFilter) {
        ObjectNode result = withClassType(ExpressionConstants.CONJUNCTION_CLASS, ExpressionConstants.CONJUNCTION_TYPE_OR);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        arrayNode.add(leftFilter);
        arrayNode.add(rightFilter);
        result.set("children", arrayNode);
        return result;
    }

    public static JsonNode createFunction(String name, String schema, String catalog, JsonNode children) {
        var orderBy = new ObjectNode(JsonNodeFactory.instance);
        orderBy.put("type", "ORDER_MODIFIER");
        orderBy.set("orders", new ArrayNode(JsonNodeFactory.instance));
        var input =withClassType(FUNCTION_CLASS, FUNCTION_TYPE);
        input.put("function_name", name);
        input.put("schema", schema);
        input.put("catalog",  catalog);
        input.put("distinct", false);
        input.put("is_operator", false);
        input.put("export_state", false);
        input.put("catalog", "");
        input.set("children", children);
        input.set("filter", null);
        input.set("order_bys", orderBy);
        return input;
    }

    private static JsonNode constantValueNode(Object value) {
        ObjectNode valueNode = new ObjectNode(JsonNodeFactory.instance);
        ObjectNode type = new ObjectNode(JsonNodeFactory.instance);
        valueNode.set("type", type);
        type.set("type_info", null);
        if (value == null) {
            valueNode.put("is_null", true);
            type.put("id", "NULL");
        } else {
            valueNode.put("is_null", false);
            String id;
            if (value instanceof String string) {
                type.put("id", "VARCHAR");
                valueNode.put("value", string);
            } else {
                throw new RuntimeException("Unsupported " + value);
            }
        }
        return valueNode;
    }

    private static ObjectNode withClassType(String clazz, String type) {
        ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        result.put("class", clazz);
        result.put("type", type);
        result.put("alias", "");
        result.put("query_location", 0);
        return result;
    }

    private static JsonNode comparing(String type, JsonNode left, JsonNode right) {
        ObjectNode node = withClassType(ExpressionConstants.COMPARISON_CLASS, type);
        node.set("left", left);
        node.set("right", right);
        return node;
    }
}
