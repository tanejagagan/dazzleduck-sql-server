package io.dazzleduck.sql.logback;

import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LogToArrowConverterTest {

    private LogToArrowConverter converter;

    @BeforeEach
    void setUp() {
        converter = new LogToArrowConverter();
    }

    @AfterEach
    void tearDown() {
        converter.close();
    }

    @Test
    void getSchema_shouldReturnValidSchema() {
        Schema schema = converter.getSchema();

        assertNotNull(schema);
        assertEquals(7, schema.getFields().size());
        assertNotNull(schema.findField("s_no"));
        assertNotNull(schema.findField("timestamp"));
        assertNotNull(schema.findField("level"));
        assertNotNull(schema.findField("logger"));
        assertNotNull(schema.findField("thread"));
        assertNotNull(schema.findField("message"));
        assertNotNull(schema.findField("mdc"));
    }

    @Test
    void convertToArrowBytes_shouldReturnNullForNullInput() {
        byte[] result = converter.convertToArrowBytes(null);
        assertNull(result);
    }

    @Test
    void convertToArrowBytes_shouldReturnNullForEmptyList() {
        byte[] result = converter.convertToArrowBytes(List.of());
        assertNull(result);
    }

    @Test
    void convertToArrowBytes_shouldConvertSingleEntry() throws IOException {
        Instant now = Instant.parse("2024-01-15T10:30:00Z");
        LogEntry entry = new LogEntry(
                1,
                now,
                "INFO",
                "com.example.Test",
                "main",
                "Test message"
        );

        byte[] arrowBytes = converter.convertToArrowBytes(List.of(entry));

        assertNotNull(arrowBytes);
        assertTrue(arrowBytes.length > 0);

        // Verify the content by reading it back
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            assertTrue(reader.loadNextBatch());

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(1, root.getRowCount());

            TimeStampMilliVector timestampVec = (TimeStampMilliVector) root.getVector("timestamp");
            assertEquals(now.toEpochMilli(), timestampVec.get(0));
            assertEquals("INFO", getString(root, "level", 0));
            assertEquals("com.example.Test", getString(root, "logger", 0));
            assertEquals("main", getString(root, "thread", 0));
            assertEquals("Test message", getString(root, "message", 0));
        }
    }

    @Test
    void convertToArrowBytes_shouldConvertMultipleEntries() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries.add(new LogEntry(
                    i,
                    Instant.now(),
                    i % 2 == 0 ? "INFO" : "ERROR",
                    "Logger" + i,
                    "thread-" + i,
                    "Message " + i
            ));
        }

        byte[] arrowBytes = converter.convertToArrowBytes(entries);

        assertNotNull(arrowBytes);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            assertTrue(reader.loadNextBatch());

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(5, root.getRowCount());

            for (int i = 0; i < 5; i++) {
                assertEquals("Message " + i, getString(root, "message", i));
                assertEquals("Logger" + i, getString(root, "logger", i));
            }
        }
    }

    @Test
    void convertToArrowBytes_shouldHandleNullValues() throws IOException {
        LogEntry entry = new LogEntry(
                1,
                null,  // null timestamp
                "WARN",
                null,  // null logger
                "main",
                "Test message"
        );

        byte[] arrowBytes = converter.convertToArrowBytes(List.of(entry));

        assertNotNull(arrowBytes);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            assertTrue(reader.loadNextBatch());

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(1, root.getRowCount());

            TimeStampMilliVector timestampVec = (TimeStampMilliVector) root.getVector("timestamp");
            VarCharVector loggerVec = (VarCharVector) root.getVector("logger");

            assertTrue(timestampVec.isNull(0));
            assertTrue(loggerVec.isNull(0));

            assertEquals("WARN", getString(root, "level", 0));
            assertEquals("Test message", getString(root, "message", 0));
        }
    }

    @Test
    void convertToVectorSchemaRoot_shouldReturnValidRoot() {
        LogEntry entry = new LogEntry(
                1,
                Instant.now(),
                "DEBUG",
                "TestLogger",
                "main",
                "Debug message"
        );

        try (VectorSchemaRoot root = converter.convertToVectorSchemaRoot(List.of(entry))) {
            assertNotNull(root);
            assertEquals(1, root.getRowCount());
            assertEquals("DEBUG", getString(root, "level", 0));
            assertEquals("Debug message", getString(root, "message", 0));
        }
    }

    @Test
    void convertToVectorSchemaRoot_shouldReturnNullForEmptyList() {
        VectorSchemaRoot result = converter.convertToVectorSchemaRoot(List.of());
        assertNull(result);
    }

    @Test
    void convertToArrowBytes_shouldHandleLargeMessages() throws IOException {
        StringBuilder largeMessage = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeMessage.append("x");
        }

        LogEntry entry = new LogEntry(
                1,
                Instant.now(),
                "INFO",
                "TestLogger",
                "main",
                largeMessage.toString()
        );

        byte[] arrowBytes = converter.convertToArrowBytes(List.of(entry));

        assertNotNull(arrowBytes);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            assertTrue(reader.loadNextBatch());

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            String message = getString(root, "message", 0);
            assertEquals(10000, message.length());
        }
    }

    @Test
    void convertToArrowBytes_shouldIncludeMdcValues() throws IOException {
        Map<String, String> mdc = Map.of("request_id", "REQ-123", "user_id", "alice");
        LogEntry entry = new LogEntry(
                1,
                Instant.now(),
                "INFO",
                "TestLogger",
                "main",
                "Test with MDC",
                mdc
        );

        byte[] arrowBytes = converter.convertToArrowBytes(List.of(entry));

        assertNotNull(arrowBytes);
        assertTrue(arrowBytes.length > 0);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {

            assertTrue(reader.loadNextBatch());

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(1, root.getRowCount());
            assertEquals("Test with MDC", getString(root, "message", 0));
            // MDC map is present (we verified schema includes it)
            assertNotNull(root.getVector("mdc"));
        }
    }

    private String getString(VectorSchemaRoot root, String fieldName, int index) {
        VarCharVector vector = (VarCharVector) root.getVector(fieldName);
        if (vector.isNull(index)) {
            return null;
        }
        return new String(vector.get(index));
    }
}
