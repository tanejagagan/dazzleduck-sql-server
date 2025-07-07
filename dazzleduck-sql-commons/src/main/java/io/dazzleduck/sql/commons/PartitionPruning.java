package io.dazzleduck.sql.commons;

import java.util.Map;


public abstract class PartitionPruning {

    public String singleQuote(String col) {
        return String.format("'%s'", col);
    }

    String extract(Map.Entry<String, String> e, String map, String aliasPrefix) {
        StringBuilder sb = new StringBuilder();
        sb.append("cast(map_extract(")
                .append(map)
                .append(singleQuote(e.getKey()))
                .append(String.format(") as %s )", e.getValue()))
                .append(String.format("as %s_%s", aliasPrefix, e.getKey()));
        return sb.toString();
    }

    public String generateDeltaCTE(Map<String, String> dataTypeMap, String source) {
        String minMapName = "mins";
        String maxMapName = "maxs";
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select filename,");
        for (Map.Entry<String, String> e : dataTypeMap.entrySet()) {
            String mins = extract(e, minMapName, "min");
            String maxs = extract(e, maxMapName, "max");
            stringBuilder.append(mins).append(",").append(maxs);
        }
        stringBuilder.append("from ")
                .append(source);
        return stringBuilder.toString();
    }

    public static String extractHivePartitions(String cname, String ctype, int split) {
        return String.format("cast( split[%s] as %s) as %s", split, ctype, cname);
    }

    public static String generateHiveCTE(String[][] partitionDataTypes, String source) {
        StringBuilder stringBuilder = new StringBuilder();
        int splitPositionStart = 0;
        for (int i = 0; i < partitionDataTypes.length; i++) {
            String key = partitionDataTypes[i][0];
            String dataType = partitionDataTypes[i][1];
            int splitPos = splitPositionStart + i;
            stringBuilder.append(
                    extractHivePartitions(key, dataType, splitPos));
            stringBuilder.append(",");
        }
        stringBuilder.append("(");
        stringBuilder.append(source);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }
}
