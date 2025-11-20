package io.dazzleduck.sql.commons;

import java.sql.SQLException;

public class RuntimeSqlException extends RuntimeException {
    final SQLException sqlException;
    public RuntimeSqlException(SQLException sqlException){
        super(sqlException);
        this.sqlException = sqlException;
    }
}
