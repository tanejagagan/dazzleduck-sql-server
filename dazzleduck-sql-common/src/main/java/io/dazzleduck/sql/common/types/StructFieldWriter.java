package io.dazzleduck.sql.common.types;

import org.apache.arrow.vector.complex.writer.BaseWriter;

import java.util.Map;

public interface StructFieldWriter {
    void write(BaseWriter.StructWriter sw, String fieldName, Object value);

    StructFieldWriter VARCHAR = (sw, name, value) -> {
        if (value == null) {
            sw.varChar(name).writeNull();
        } else {
            sw.varChar(name).writeVarChar((String) value);
        }
    };

    StructFieldWriter BIGINT = (sw, name, value) -> {
        if (value == null) {
            sw.bigInt(name).writeNull();
        } else {
            sw.bigInt(name).writeBigInt((Long) value);
        }
    };

    StructFieldWriter DOUBLE = (sw, name, value) -> {
        if (value == null) {
            sw.float8(name).writeNull();
        } else {
            sw.float8(name).writeFloat8((Double) value);
        }
    };

    StructFieldWriter STR_MAP = (sw, name, value) -> {
        BaseWriter.MapWriter mw = sw.map(name, false);
        if (value == null) {
            mw.writeNull();
            return;
        }
        mw.startMap();
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) value;
        for (Map.Entry<String, String> e : map.entrySet()) {
            mw.startEntry();
            ElementWriteFunction.VARCHAR.apply((BaseWriter.ListWriter) mw.key(), e.getKey());
            ElementWriteFunction.VARCHAR.apply((BaseWriter.ListWriter) mw.value(), e.getValue());
            mw.endEntry();
        }
        mw.endMap();
    };
}
