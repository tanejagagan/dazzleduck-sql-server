package io.dazzleduck.sql.commons;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface Extractor<T> {

    T extract(ResultSet rs) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException;
    
    default T apply(ResultSet rs) {
        try {
            return extract(rs);
        } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}