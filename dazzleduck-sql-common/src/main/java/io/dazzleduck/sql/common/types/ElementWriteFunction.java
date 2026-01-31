package io.dazzleduck.sql.common.types;

import org.apache.arrow.vector.complex.writer.BaseWriter;

public interface ElementWriteFunction {

    void apply(BaseWriter.ListWriter listWriter, Object o);

    ElementWriteFunction INT = (listWriter, o) -> {
        if (o == null) {
            listWriter.integer().writeNull();
        } else {
            listWriter.integer().writeInt((Integer) o);
        }
    };

    ElementWriteFunction BIGINT = (listWriter, o) -> {
        if (o == null) {
            listWriter.bigInt().writeNull();
        } else {
            listWriter.bigInt().writeBigInt((Long) o);
        }
    };

    ElementWriteFunction VARCHAR = (listWriter, o) -> {
        if (o == null) {
            listWriter.varChar().writeNull();
        } else {
            listWriter.varChar().writeVarChar((String) o);
        }
    };

    ElementWriteFunction DOUBLE = (listWriter, o) -> {
        if (o == null) {
            listWriter.float8().writeNull();
        } else {
            listWriter.float8().writeFloat8((Double) o);
        }
    };
}
