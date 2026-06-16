package io.dazzleduck.sql.commons.io;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultStreamsTest {

    // Two rows, including a DATE column to exercise the temporal formatValue path.
    private static final String SQL =
            "SELECT * FROM (VALUES (1, 'a', DATE '2020-01-02'), (2, 'b', DATE '2020-01-03')) "
                    + "t(id, name, d) ORDER BY id";

    private interface ReaderConsumer<T> {
        T apply(ArrowReader reader) throws SQLException, IOException;
    }

    /** Opens a fresh reader for SQL, applies fn, and closes everything. */
    private static <T> T withReader(ReaderConsumer<T> fn) throws SQLException, IOException {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, SQL, 1024)) {
            return fn.apply(reader);
        }
    }

    /** Drains an Arrow IPC stream and returns the row count. */
    private static long readArrowRows(byte[] bytes, CompressionUtil.CodecType codec) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = codec == CompressionUtil.CodecType.NO_COMPRESSION
                     ? new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
                     : new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator,
                             CommonsCompressionFactory.INSTANCE)) {
            long rows = 0;
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
            }
            return rows;
        }
    }

    @Test
    void writeTsvWritesHeaderRowsAndIsoTemporal() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long rows = withReader(r -> ResultStreams.writeTsv(r, out));

        assertEquals(2, rows);
        String[] lines = out.toString(StandardCharsets.UTF_8).strip().split("\n");
        assertEquals("id\tname\td", lines[0]);
        assertEquals("1\ta\t2020-01-02", lines[1]); // DATE -> ISO-8601 via formatValue
        assertEquals("2\tb\t2020-01-03", lines[2]);
    }

    @Test
    void writeArrowUncompressedRoundTrips() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long rows = withReader(r -> ResultStreams.writeArrow(
                r, out, CompressionUtil.CodecType.NO_COMPRESSION, null));

        assertEquals(2, rows);
        assertEquals(2, readArrowRows(out.toByteArray(), CompressionUtil.CodecType.NO_COMPRESSION));
    }

    @Test
    void writeArrowZstdRoundTrips() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long rows = withReader(r -> ResultStreams.writeArrow(
                r, out, CompressionUtil.CodecType.ZSTD, CommonsCompressionFactory.INSTANCE));

        assertEquals(2, rows);
        // Decodes only with a compression-aware reader.
        assertEquals(2, readArrowRows(out.toByteArray(), CompressionUtil.CodecType.ZSTD));
        assertTrue(out.size() > 0);
    }
}
