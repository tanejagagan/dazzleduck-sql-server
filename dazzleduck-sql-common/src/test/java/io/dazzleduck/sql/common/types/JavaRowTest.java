package io.dazzleduck.sql.common.types;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JavaRowTest {

    @Test
    public void testGetActualSizeWithNull() {
        JavaRow row = new JavaRow(null);
        long size = row.getActualSize();
        // Should have no size since we exclude overhead
        assertEquals(0, size, "Null objects array should have zero size");
    }

    @Test
    public void testGetActualSizeWithEmptyArray() {
        JavaRow row = new JavaRow(new Object[0]);
        long size = row.getActualSize();
        // Empty array has no objects, so size should be 0 (overhead excluded)
        assertEquals(0, size, "Empty array should have zero size");
    }

    @Test
    public void testGetActualSizeWithPrimitiveWrappers() {
        Object[] objects = new Object[]{
            42,                    // Integer: 16 + 4 = 20
            3.14,                  // Double: 16 + 8 = 24
            100L,                  // Long: 16 + 8 = 24
            2.5f,                  // Float: 16 + 4 = 20
            true,                  // Boolean: 16 + 4 = 20
            (byte) 10,             // Byte: 16 + 4 = 20
            (short) 200,           // Short: 16 + 4 = 20
            'A'                    // Character: 16 + 4 = 20
        };
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Expected: 20 + 24 + 24 + 20 + 20 + 20 + 20 + 20 = 168 bytes
        assertEquals(168, size, "Primitive wrappers should total 168 bytes");
    }

    @Test
    public void testGetActualSizeWithStrings() {
        Object[] objects = new Object[]{
            "Hello",
            "World",
            "Test"
        };
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Each string: object header + char array overhead + chars
        // "Hello": 16 + 16 + (5 * 2) = 42
        // "World": 16 + 16 + (5 * 2) = 42
        // "Test": 16 + 16 + (4 * 2) = 40
        // Total: 42 + 42 + 40 = 124 bytes
        assertEquals(124, size, "String sizes should total 124 bytes");
    }

    @Test
    public void testGetActualSizeWithByteArray() {
        byte[] data = new byte[100];
        Object[] objects = new Object[]{data};
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // byte[100]: 16 (object header) + 16 (array overhead) + 100 (bytes) = 132
        assertEquals(132, size, "Byte array should be exactly 132 bytes");
    }

    @Test
    public void testGetActualSizeWithBigDecimal() {
        Object[] objects = new Object[]{
            new BigDecimal("123.456"),
            new BigDecimal("999999.999999")
        };
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // BigDecimal has significant overhead
        assertTrue(size > 100, "BigDecimal should have overhead for internal representation");
    }

    @Test
    public void testGetActualSizeWithDates() {
        Object[] objects = new Object[]{
            LocalDate.now(),
            LocalTime.now(),
            LocalDateTime.now()
        };
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Date/Time objects have fields for year, month, day, hour, minute, second, nano
        assertTrue(size > 50, "Date/Time objects should have measurable size");
    }

    @Test
    public void testGetActualSizeWithNestedArray() {
        Object[] nested = new Object[]{"A", "B", "C"};
        Object[] objects = new Object[]{nested};
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Nested array: 16 (obj header) + 16 (array overhead) + (3 * 8 refs) = 40
        // "A": 16 + 16 + 2 = 34
        // "B": 16 + 16 + 2 = 34
        // "C": 16 + 16 + 2 = 34
        // Total: 16 + 40 + 34 + 34 + 34 = 158
        assertEquals(158, size, "Nested array should be exactly 158 bytes");
    }

    @Test
    public void testGetActualSizeWithList() {
        List<String> list = new ArrayList<>();
        list.add("Item1");
        list.add("Item2");
        list.add("Item3");
        Object[] objects = new Object[]{list};
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // List: 16 (obj header) + 16 (backing array) + (3 * 8 refs) = 40
        // "Item1": 16 + 16 + 10 = 42
        // "Item2": 16 + 16 + 10 = 42
        // "Item3": 16 + 16 + 10 = 42
        // Total: 16 + 40 + 42 + 42 + 42 = 182
        assertEquals(182, size, "List should be exactly 182 bytes");
    }

    @Test
    public void testGetActualSizeWithMap() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        Object[] objects = new Object[]{map};
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Map: 16 (obj header) + keys/values (no map overhead)
        // "key1": 16 + 16 + 8 = 40
        // "value1": 16 + 16 + 12 = 44
        // "key2": 16 + 16 + 8 = 40
        // "value2": 16 + 16 + 12 = 44
        // Total: 16 + 40 + 44 + 40 + 44 = 184
        assertEquals(184, size, "Map should be exactly 184 bytes");
    }

    @Test
    public void testGetActualSizeWithNullElements() {
        Object[] objects = new Object[]{null, null, null};
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Null elements contribute nothing (overhead excluded)
        assertEquals(0, size, "Null elements should contribute zero size");
    }

    @Test
    public void testGetActualSizeWithMixedTypes() {
        Object[] objects = new Object[]{
            "String",                       // 16 + 16 + 12 = 44
            42,                             // 16 + 4 = 20
            3.14,                           // 16 + 8 = 24
            true,                           // 16 + 4 = 20
            new byte[]{1, 2, 3, 4, 5},     // 16 + 16 + 5 = 37
            LocalDate.now()                 // 16 + 8 = 24
        };
        JavaRow row = new JavaRow(objects);
        long size = row.getActualSize();

        // Total: 44 + 20 + 24 + 20 + 37 + 24 = 169
        assertEquals(169, size, "Mixed types should total 169 bytes");
    }

    @Test
    public void testGetActualSizeIsConsistent() {
        Object[] objects = new Object[]{"Test", 123, 4.56};
        JavaRow row = new JavaRow(objects);

        long size1 = row.getActualSize();
        long size2 = row.getActualSize();

        assertEquals(size1, size2, "Multiple calls should return same size");
    }

    @Test
    public void testGetActualSizeAfterModification() {
        Object[] objects = new Object[]{"Short", 1};
        JavaRow row = new JavaRow(objects);

        long initialSize = row.getActualSize();

        // Modify the row
        row.set(0, "A much longer string that should increase the size");

        long newSize = row.getActualSize();

        assertTrue(newSize > initialSize, "Size should increase after setting larger value");
    }

    @Test
    public void testGetActualSizeWithUnsupportedType() {
        // Create an object of an unsupported type
        class CustomObject {
            int value = 42;
        }
        Object[] objects = new Object[]{new CustomObject()};
        JavaRow row = new JavaRow(objects);

        // Should throw UnsupportedOperationException for unknown types
        assertThrows(UnsupportedOperationException.class, row::getActualSize,
                "Should throw exception for unsupported type");
    }
}
