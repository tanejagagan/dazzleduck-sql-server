package io.dazzleduck.sql.commons.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public interface DataType {

    String toSql();

    public static class Field {
        public String name;
        public final DataType dataType;
        
        public Field(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Field f &&
                    f.name.equals(name) &&
                    f.dataType.equals(dataType);
        }
    }


    public static Bigint BIGINT = new Bigint();
    public static Bit BIT = new Bit();
    public static Blob BLOB = new Blob();
    public static Bool BOOLEAN = new Bool();
    public static Date DATE = new Date();
    public static Double DOUBLE = new Double();
    public static Float FLOAT = new Float();
    public static Integer INTEGER = new Integer();
    public static Interval INTERVAL = new Interval();
    public static SmallInt SMALLINT = new SmallInt();
    public static Timestamp TIMESTAMP = new Timestamp();
    public static Timestampz TIMESTAMPZ = new Timestampz();
    public static Varchar VARCHAR = new Varchar();
    public static NullType NULL = new NullType();


    static class NullType implements DataType {

        @Override
        public String toSql() {
            return "null";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NullType;
        }
    }
    static class Bool implements DataType {
        public String toSql() {
            return "BOOLEAN";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Bool;
        }
    }
    static class Blob implements DataType {

        @Override
        public String toSql() {
            return "BLOB";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Blob;
        }
    }

    static class Bit implements DataType {
        @Override
        public String toSql() {
            return "BIT";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Bit;
        }
    }
    static class TinyInt implements  DataType {

        @Override
        public String toSql() {
            return "TINYINT";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TinyInt;
        }
    }

    static class Integer implements DataType {

        @Override
        public String toSql() {
            return "INTEGER";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Integer;
        }
    }

    public static class Bigint implements DataType {

        @Override
        public String toSql() {
            return "BIGINT";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Bigint;
        }
    }
    
    public static class Varchar implements DataType {

        @Override
        public String toSql() {
            return "VARCHAR";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Varchar;
        }
    }

    public static class SmallInt implements DataType {

        @Override
        public String toSql() {
            return "SMALLINT";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SmallInt;
        }
    }

    public static class Float implements  DataType {

        @Override
        public String toSql() {
            return "FLOAT";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Float;
        }
    }

    public static class  Double implements DataType {

        @Override
        public String toSql() {
            return "DOUBLE";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Float;
        }
    }


    public static class  Date implements  DataType {

        @Override
        public String toSql() {
            return "DATE";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Date;
        }
    }

    public static class Time implements DataType {

        @Override
        public String toSql() {
            return "TIME";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Time;
        }
    }

    public static class Timestamp implements  DataType {

        @Override
        public String toSql() {
            return "TIMESTAMP";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Timestamp;
        }
    }

    public static class Timestampz implements  DataType {

        @Override
        public String toSql() {
            return "TIMESTAMPTZ";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Timestampz;
        }
    }

    public static class Interval implements  DataType {

        @Override
        public String toSql() {
            return "INTERVAL";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Interval;
        }
    }

    public static class Decimal implements DataType {
        public final int width;
        public final int scale;
        public Decimal(int width, int scale) {
            this.width = width;
            this.scale = scale;
        }

        @Override
        public String toSql() {
            return String.format("DECIMAL(%s,%s)", width, scale);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Decimal d && d.width == width && d.scale == scale;
        }
    }

    public static class Struct implements DataType {
        public final ArrayList<Field> fields;

        public Struct() {
            this.fields = new ArrayList<>();
        }

        public Struct(List<Field> fields) {
            this.fields = new ArrayList<>(fields);
        }
        
        public Struct add(String[] columnNames, DataType dataType) {
            Struct current = this;
            for(int i = 0 ; i < columnNames.length -1 ; i ++) {
                current = (Struct) current.getOrAdd(columnNames[i], new Struct());
            }
            current.getOrAdd(columnNames[columnNames.length -1], dataType);
            return this;
        }

        public Struct add(String columnName, DataType dataType) {
            fields.add(new Field(columnName, dataType));
            return this;
        }
        
        public DataType getOrAdd(final String name,
                                 DataType type) {
            DataType f = get(name);
            if(f != null) {
                return f;
            }
            fields.add(new Field(name, type));
            return type;
        }

        public DataType get(final  String name) {
            for (Field current : fields) {
                if (current.name.equals(name)) {
                    return current.dataType;
                }
            }
            return null;
        }

        @Override
        public String toSql() {
            String fieldsString = fields.stream()
                    .map( f -> String.format("%s %s", quoteIdentifier(f.name),
                            f.dataType.toSql()))
                    .collect(Collectors.joining(","));
            return String.format("STRUCT(%s)", fieldsString);
        }

        @Override
        public String toString(){
            return toSql();
        }

        @Override
        public boolean equals(Object obj) {
            if( obj instanceof Struct s && s.fields.size() == fields.size()) {
                for(int i =0 ; i < fields.size(); i ++) {
                    if(!s.fields.get(i).equals(fields.get(i))){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        public boolean hasNullType() {
            for(Field f : fields){
                if(f.dataType instanceof Struct s){
                    return s.hasNullType();
                }
                if(f.dataType.equals(NULL)) {
                    return true;
                }
            }
            return false;
        }

        private String quoteIdentifier(String identifier) {
            return String.format("\"%s\"", identifier);
        }
    }
    
    public static class ArrayType implements  DataType {
        public final DataType internal;
        public ArrayType(DataType internal) {
            this.internal = internal;
        }

        @Override
        public String toSql() {
            return String.format("%s[]", internal.toSql());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ArrayType a && a.internal.equals(this.internal);
        }
    }
    
    public static class MapType implements  DataType {
        public final DataType keyType;
        public final DataType valueType;
        
        public MapType(DataType keyType, DataType valueType) {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public String toSql() {
            return String.format("MAP(%s, %s)", keyType.toSql(), valueType.toSql());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MapType m &&
                    m.keyType.equals(keyType) &&
                    m.valueType.equals(valueType);
        }
    }

    public static DataType getDataType(JsonNode jsonNode) {
        String id = jsonNode.get("id").asText();
        switch (id) {
            case "BIGINT" : return BIGINT;
            case "BIT" : return  BIT;
            case "BLOB" : return BLOB;
            case "BOOLEAN" : return BOOLEAN;
            case "DATE" : return DATE;
            case "DOUBLE" : return DOUBLE;
            case "FLOAT" : return FLOAT;
            case "INTEGER" : return INTEGER;
            case "SMALLINT" : return SMALLINT;
            case "TIMESTAMP" : return TIMESTAMP;
            case "VARCHAR" : return VARCHAR;

            case "LIST" :
                JsonNode childType = jsonNode.get("type_info").get("child_type");
                DataType childDataType = getDataType(childType);
                return new ArrayType(childDataType);

            case "STRUCT" :
                ArrayNode childTypes = (ArrayNode) jsonNode.get("type_info").get("child_types");
                ArrayList<Field> fields = new ArrayList<>();
                for (Iterator<JsonNode> it = childTypes.elements(); it.hasNext(); ) {
                    JsonNode child = it.next();
                    String first = child.get("first").asText();
                    DataType second = getDataType(child.get("second"));
                    Field f = new Field(first, second);
                    fields.add(f);
                }
                return new Struct(fields);

            case "DECIMAL" :
                JsonNode typeInfo = jsonNode.get("type_info");
                int width = typeInfo.get("width").asInt();
                int scale = typeInfo.get("scale").asInt();
                return new Decimal(width, scale);
                default: throw new UnsupportedOperationException("datatype " + id + "Not supported") ;
        }
    }
}
