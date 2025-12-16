package io.dazzleduck.sql.common.ingestion;

import io.dazzleduck.sql.common.types.JavaRow;

import java.util.ArrayList;
import java.util.List;

public class Bucket {

    private List<JavaRow> buffer = new ArrayList<>();
    private long size = 0;

    private boolean serialized = false;
    public synchronized long add(JavaRow row) {
        if(serialized) {
            throw new IllegalStateException("the bucket is already serialized");
        }
        buffer.add(row);
        size += row.getActualSize();
        return size;
    }

    public synchronized byte[] getArrowBytes(){
        serialized = true;
        return null;
    }

    public synchronized long size(){
        return size;
    }
}
