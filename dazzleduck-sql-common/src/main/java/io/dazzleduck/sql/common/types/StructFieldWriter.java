package io.dazzleduck.sql.common.types;

import org.apache.arrow.vector.complex.writer.BaseWriter;

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
}
