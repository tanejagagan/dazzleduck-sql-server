package io.dazzleduck.sql.search;

import org.apache.arrow.vector.ipc.ArrowReader;

public interface Split {
    ArrowReader readResult(String sql,
                           String[] indexFiles);
}

