package io.dazzleduck.sql.common.types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class JavaRow {
    private final Object[] objects;

    public JavaRow(Object[] objects) {
        this.objects = objects;
    }

    public Object[] objects() {
        return objects;
    }

    public Object get(int index) {
        return objects[index];
    }

    public void set(int index, Object object) {
        objects[index] = object;
    }

    /**
     * Calculates the approximate memory size of the data contained in this JavaRow in bytes.
     * This excludes the JavaRow object overhead and array overhead, focusing only on the
     * actual data size of the contained objects.
     *
     * @return the approximate size in bytes of the contained data
     */
    public long getActualSize() {
        long size = 0;

        if (objects == null) {
            return size;
        }

        // Calculate size of each object in the array (excluding structural overhead)
        for (Object obj : objects) {
            size += estimateObjectSize(obj);
        }

        return size;
    }

    /**
     * Estimates the memory size of a single object.
     *
     * @param obj the object to estimate
     * @return the approximate size in bytes
     */
    private long estimateObjectSize(Object obj) {
        if (obj == null) {
            return 0;
        }

        // Object header (12 bytes) + padding/alignment (4 bytes minimum)
        long size = 16;

        if (obj instanceof String) {
            String str = (String) obj;
            // char[] array: header + length + (2 bytes per character)
            size += 16 + ((long) str.length() * 2);
        } else if (obj instanceof Integer || obj instanceof Float) {
            // Wrapper objects: header + 4 bytes for value
            size += 4;
        } else if (obj instanceof Long || obj instanceof Double) {
            // Wrapper objects: header + 8 bytes for value
            size += 8;
        } else if (obj instanceof Boolean || obj instanceof Byte) {
            // Wrapper objects: header + 1 byte for value + padding
            size += 4;
        } else if (obj instanceof Short || obj instanceof Character) {
            // Wrapper objects: header + 2 bytes for value + padding
            size += 4;
        } else if (obj instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) obj;
            // BigDecimal: header + BigInteger reference + scale + precision
            size += 32;
            // Approximate BigInteger size
            size += 32 + (bd.precision() / 2);
        } else if (obj instanceof LocalDate) {
            // LocalDate: header + year (4) + month (2) + day (2)
            size += 8;
        } else if (obj instanceof LocalTime) {
            // LocalTime: header + hour, minute, second, nano
            size += 16;
        } else if (obj instanceof LocalDateTime) {
            // LocalDateTime: header + LocalDate reference + LocalTime reference
            size += 16 + estimateObjectSize(LocalDate.now()) + estimateObjectSize(LocalTime.now());
        } else if (obj instanceof byte[]) {
            byte[] byteArray = (byte[]) obj;
            // byte[]: header + length + bytes
            size += 16 + byteArray.length;
        } else if (obj instanceof Object[]) {
            Object[] objArray = (Object[]) obj;
            // Object[]: header + length + references + recursive estimation
            size += 16 + ((long) objArray.length * 8);
            for (Object element : objArray) {
                size += estimateObjectSize(element);
            }
        } else if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            // Estimate the backing array and elements
            size += 16 + ((long) list.size() * 8);
            for (Object element : list) {
                size += estimateObjectSize(element);
            }
        } else if (obj instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) obj;
            // Estimate entries (each Entry has header + key + value + hash + next reference)
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size += estimateObjectSize(entry.getKey());
                size += estimateObjectSize(entry.getValue());
            }
        } else {
            throw new UnsupportedOperationException("Not supported datatype " + obj.getClass());
        }

        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JavaRow javaRow = (JavaRow) o;
        return Arrays.equals(objects, javaRow.objects);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(objects);
    }

    @Override
    public String toString() {
        return "JavaRow[objects=" + Arrays.toString(objects) + "]";
    }
}
