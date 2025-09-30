package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.common.auth.UnauthorizedException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class SimpleAuthorizer implements SqlAuthorizer {
    private record AccessKey(String user, Transformations.CatalogSchemaTable catalogSchemaTable) { }
    private record AccessValue(JsonNode filter, List<String> columns) { }
    Map<AccessKey, AccessValue> accessMap = new HashMap<>();
    Set<String> superUsers = new HashSet<>();

    public static final String ACCESS_FILE = "simple-access.json";

    public SimpleAuthorizer(Map<String, List<String>> userGroupMapping,
                            List<AccessRow> accessRows) {
        var groupAccessRowMap = new HashMap<String, List<AccessRow>>();
        for (var row : accessRows) {
            groupAccessRowMap.compute(row.group(), (key, oldValue) -> {
                if (oldValue == null) {
                    var l = new ArrayList<AccessRow>();
                    l.add(row);
                    return l;
                } else {
                    oldValue.add(row);
                    return oldValue;
                }
            });
        }

        userGroupMapping.forEach((user, groups) -> {
            var map = new HashMap<Transformations.CatalogSchemaTable, AccessValue>();
            groups.forEach(group -> {
                var _accessRows = groupAccessRowMap.get(group);
                if(_accessRows != null) {
                    _accessRows.forEach(accessRow -> {
                        var key = new Transformations.CatalogSchemaTable(accessRow.database(), accessRow.schema(), accessRow.tableOrPath(), accessRow.tableType(), accessRow.functionName());
                        map.compute(key, (k, oldValue) -> {
                            if (oldValue == null) {
                                return collapse(accessRow);
                            } else {
                                return collapse(accessRow, oldValue);
                            }
                        });

                    });
                }
            });
            map.forEach((key, value) -> accessMap.put(new AccessKey(user, key), value));
        });
    }

    public static SqlAuthorizer load(Config config) throws IOException {
        var userGroupMapping = loadUsrGroupMapping(config);
        var accessRows = loadAccessRows();
        return new SimpleAuthorizer(userGroupMapping, accessRows);
    }

    public static List<AccessRow> loadAccessRows() throws IOException {
        var result = new ArrayList<AccessRow>();
        try (InputStream inputStream = SimpleAuthorizer.class.getResourceAsStream("/" + ACCESS_FILE);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            ObjectMapper mapper = new ObjectMapper();
            var line = reader.readLine();
            var accessRow = mapper.readValue(line, AccessRow.class);
            result.add(accessRow);
        }
        return result;
    }

    public static Map<String, List<String>> loadUsrGroupMapping(Config conf ) throws IOException {
        var res = new HashMap<String, List<String>>();
        var users = conf.getObjectList("users");
        for (var userConfigObject : users) {
            var userConfig = userConfigObject.toConfig();
            var user = userConfig.getString("username");
            var groups = userConfig.getStringList("groups");
            res.put(user, groups);
        }
        return res;
    }

    private AccessValue collapse(AccessRow r1, AccessValue accessValue) {
        return new AccessValue(collapseFilters(r1, accessValue.filter), collapseColumns(r1, accessValue.columns));
    }

    private AccessValue collapse(AccessRow r) {
        return new AccessValue(SqlAuthorizer.compileFilterString(r.filter()), r.columns());
    }

    private List<String> collapseColumns(AccessRow r1, List<String> columns) {
        return List.of();
    }

    private JsonNode collapseFilters(AccessRow r1, JsonNode node) {
        var qnode = SqlAuthorizer.compileFilterString(r1.filter());
        return ExpressionFactory.orFilters(qnode, node);
    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        if (superUsers.contains(user)) {
            return query;
        }
        //validateForAuthorization(query);
        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.size() != 1) {
            throw new UnauthorizedException("%s TableOrPath/Path found: Only one table or path is supported".formatted(catalogSchemaTables.size()));
        }
        var catalogSchemaTable = catalogSchemaTables.get(0);

        var key = new AccessKey(user, catalogSchemaTable);
        var a = accessMap.get(key);
        if (a == null) {
            throw new UnauthorizedException(database + "." + schema);
        }
        var columnAccess = hasAccessToColumns(query, a.columns());
        if (!columnAccess) {
            throw new UnauthorizedException("No access to columns specified columns");
        }

        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return SqlAuthorizer.addFilterToTableFunction(query, a.filter);
            }
            case BASE_TABLE -> {
                return SqlAuthorizer.addFilterToBaseTable(query, a.filter);
            }
            default -> {
                return null;
            }
        }
    }

    private boolean hasAccessToColumns(JsonNode query, List<String> accessColumn) {
        return true;
    }

    private static List<AccessRow> readAccessRows() throws IOException {
        String resourceName = "simple-access.json";
        ObjectMapper mapper = new ObjectMapper();
        List<AccessRow> result = new ArrayList<>();
        try (InputStream is = SimpleAuthorizer.class.getClassLoader().getResourceAsStream(resourceName)) {
            assert is != null;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    AccessRow myObject = mapper.readValue(line, AccessRow.class);
                    result.add(myObject);
                }
            }
        }
        return result;
    }
}
