package io.dazzleduck.sql.common.types;

import org.apache.arrow.vector.complex.impl.UnionListWriter;

public interface ElementWriteFunction {

    void apply(UnionListWriter listWriter, Object o);

    static ElementWriteFunction INT = (listWriter, o) -> listWriter.integer().writeInt((Integer)o);

    static ElementWriteFunction BIGINT = (listWriter, o) -> listWriter.bigInt().writeBigInt((Long)o);

    static ElementWriteFunction VARCHAR = (listWriter, o) -> listWriter.varChar().writeVarChar((String) o);

    static ElementWriteFunction DOUBLE = (listWriter, o) -> listWriter.float8().writeFloat8((Double)o);
}
