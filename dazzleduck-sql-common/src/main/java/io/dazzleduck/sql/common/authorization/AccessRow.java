package io.dazzleduck.sql.common.authorization;

import java.util.List;

public record AccessRow(String group,
                        String database,
                        String schema,
                        String tableOrPath,
                        String type,
                        List<String> columns,
                        String filter,
                        java.sql.Date expiration) {
}
