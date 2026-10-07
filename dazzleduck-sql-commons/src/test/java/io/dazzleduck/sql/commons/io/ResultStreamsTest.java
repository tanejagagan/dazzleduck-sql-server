package io.dazzleduck.sql.commons.io;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    /** Runs {@code sql} and returns its JSON Lines output; asserts the returned row count. */
    private static String jsonl(String sql, long expectedRows) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, sql, 1024)) {
            assertEquals(expectedRows, ResultStreams.writeJsonl(reader, out));
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    void writeJsonlWritesOneTypedObjectPerLine() throws Exception {
        String out = jsonl(SQL, 2);
        assertEquals("""
                {"id":1,"name":"a","d":"2020-01-02"}
                {"id":2,"name":"b","d":"2020-01-03"}
                """, out, "numbers stay numbers, DATE is ISO-8601, no array and no separators");
    }

    @Test
    void writeJsonlTypesEachValueKind() throws Exception {
        String out = jsonl("""
                SELECT 42::TINYINT AS ti, 7::SMALLINT AS si, 1234567890123::BIGINT AS bi,
                       1.5::FLOAT AS f4, 2.25::DOUBLE AS f8, true AS b, NULL::VARCHAR AS n,
                       'tab' || chr(9) || 'there "quoted"' AS s, from_hex('0102') AS bin,
                       TIME '12:34:56' AS t, TIMESTAMPTZ '2026-09-29 10:00:00+00' AS tz,
                       [1, 2, 3] AS list, {'k': 'v', 'n': 1} AS struct
                """, 1).strip();
        assertTrue(out.contains("\"ti\":42") && out.contains("\"si\":7") && out.contains("\"bi\":1234567890123"), out);
        assertTrue(out.contains("\"f4\":1.5") && out.contains("\"f8\":2.25"), out);
        assertTrue(out.contains("\"b\":true") && out.contains("\"n\":null"), out);
        assertTrue(out.contains("\"s\":\"tab\\tthere \\\"quoted\\\"\""), "strings are JSON-escaped: " + out);
        assertTrue(out.contains("\"bin\":\"AQI=\""), "binary is base64: " + out);
        assertTrue(out.contains("\"t\":\"12:34:56\""), out);
        assertTrue(out.contains("\"tz\":\"2026-09-29T10:00:00Z\""), "TZ timestamp as ISO-8601 instant: " + out);
        assertTrue(out.contains("\"list\":[1,2,3]"), "lists are nested JSON: " + out);
        assertTrue(out.contains("\"struct\":{\"k\":\"v\",\"n\":1}"), "structs are nested JSON: " + out);
        assertEquals(1, out.split("\n").length, "one line per row");
    }

    @Test
    void writeJsonlWritesNothingForAnEmptyResult() throws Exception {
        assertEquals("", jsonl("SELECT 1 AS id WHERE false", 0));
    }

    @Test
    void writeJsonlCoversTimestampDecimalAndMap() throws Exception {
        String out = jsonl("""
                SELECT TIMESTAMP '2026-01-02 03:04:05' AS ts, 12.34::DECIMAL(10, 2) AS dc,
                       123456789012345678901234567890.123::DECIMAL(38, 3) AS big,
                       5::UTINYINT AS u1, 65535::USMALLINT AS u2, 18446744073709551615::UBIGINT AS u8,
                       MAP {'a': 1, 'b': 2} AS m, MAP {1: 'x'} AS int_keys
                """, 1).strip();
        assertTrue(out.contains("\"ts\":\"2026-01-02T03:04:05\""), "non-TZ TIMESTAMP as ISO-8601: " + out);
        assertTrue(out.contains("\"dc\":12.34"), "DECIMAL as a JSON number: " + out);
        assertTrue(out.contains("\"big\":123456789012345678901234567890.123"), "wide DECIMAL keeps full precision: " + out);
        assertTrue(out.contains("\"u1\":5") && out.contains("\"u2\":65535") && out.contains("\"u8\":18446744073709551615"),
                "unsigned integers as numbers, without overflow: " + out);
        assertTrue(out.contains("\"m\":{\"a\":1,\"b\":2}"), "MAP as a JSON object, not an entry array: " + out);
        assertTrue(out.contains("\"int_keys\":{\"1\":\"x\"}"), "non-string map keys become their text: " + out);
    }

    @Test
    void writeJsonlFormatsNestedValuesLikeTopLevelOnes() throws Exception {
        String out = jsonl("""
                SELECT {'d': DATE '2026-01-02', 'tz': TIMESTAMPTZ '2026-01-02 03:04:05+00', 'n': NULL::INT} AS s,
                       [DATE '2026-01-02', NULL] AS dates,
                       [{'k': 1, 't': TIME '01:02:03'}] AS list_of_structs,
                       MAP {'when': TIMESTAMP '2026-01-02 03:04:05'} AS map_of_ts,
                       [[1, 2], [3]] AS nested_lists
                """, 1).strip();
        assertTrue(out.contains("\"s\":{\"d\":\"2026-01-02\",\"tz\":\"2026-01-02T03:04:05Z\",\"n\":null}"),
                "DATE and TZ timestamp inside a struct as ISO-8601, not epoch numbers: " + out);
        assertTrue(out.contains("\"dates\":[\"2026-01-02\",null]"), "dates inside a list: " + out);
        assertTrue(out.contains("\"list_of_structs\":[{\"k\":1,\"t\":\"01:02:03\"}]"), out);
        assertTrue(out.contains("\"map_of_ts\":{\"when\":\"2026-01-02T03:04:05\"}"), out);
        assertTrue(out.contains("\"nested_lists\":[[1,2],[3]]"), out);
    }

    @Test
    void writeJsonlFlushesPerBatchNotPerNestedValue() throws Exception {
        java.util.concurrent.atomic.AtomicInteger flushes = new java.util.concurrent.atomic.AtomicInteger();
        java.io.OutputStream counting = new java.io.FilterOutputStream(new ByteArrayOutputStream()) {
            @Override
            public void flush() throws IOException {
                flushes.incrementAndGet();
                super.flush();
            }
        };
        // 1,000 rows, each with a struct, a list and a map, in one batch.
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator,
                     "SELECT {'a': range} AS s, [range] AS l, MAP {'k': range} AS m FROM range(1000)", 2048)) {
            assertEquals(1000, ResultStreams.writeJsonl(reader, counting));
        }
        assertTrue(flushes.get() <= 3, "expected a flush per batch plus one on close, got " + flushes.get());
    }

    @Test
    void timesAndTimestampsAlwaysIncludeSeconds() throws Exception {
        // toString() would drop zero seconds ("12:00", "2024-01-01T00:00"), so the width would vary
        // by row and clients parsing with a fixed pattern would fail on whole-minute values.
        String sql = """
                SELECT TIME '12:00:00' AS t, TIMESTAMP '2024-01-01 00:00:00' AS ts, TIMESTAMP '2024-01-01 10:30:00' AS ts2,
                       TIMESTAMP '2024-01-01 10:30:05.5' AS frac, TIMESTAMPTZ '2024-01-01 00:00:00+00' AS tz,
                       {'t': TIME '08:00:00', 'ts': TIMESTAMP '2024-01-01 00:00:00'} AS nested,
                       [TIMESTAMP '2024-01-01 00:00:00'] AS listed
                """;
        String json = jsonl(sql, 1).strip();
        assertTrue(json.contains("\"t\":\"12:00:00\""), json);
        assertTrue(json.contains("\"ts\":\"2024-01-01T00:00:00\""), json);
        assertTrue(json.contains("\"ts2\":\"2024-01-01T10:30:00\""), json);
        assertTrue(json.contains("\"frac\":\"2024-01-01T10:30:05.5\""), "only the fraction digits needed: " + json);
        assertTrue(json.contains("\"tz\":\"2024-01-01T00:00:00Z\""), json);
        assertTrue(json.contains("\"nested\":{\"t\":\"08:00:00\",\"ts\":\"2024-01-01T00:00:00\"}"), json);
        assertTrue(json.contains("\"listed\":[\"2024-01-01T00:00:00\"]"), json);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator,
                     "SELECT TIME '12:00:00' AS t, TIMESTAMP '2024-01-01 00:00:00' AS ts", 1024)) {
            ResultStreams.writeTsv(reader, out);
        }
        assertEquals("t\tts\n12:00:00\t2024-01-01T00:00:00\n", out.toString(StandardCharsets.UTF_8), "TSV too");
    }

    // ENUM is dictionary-encoded in DuckDB's Arrow export, at the top level and inside lists,
    // structs and maps. %1$s is the column type: an ENUM, or VARCHAR for the expected output.
    private static final String ENUM_SQL = """
            SELECT 'ok'::%1$s AS top, NULL::%1$s AS null_value, ['happy'::%1$s, NULL] AS list,
                   {'mood': 'sad'::%1$s, 'missing': NULL::%1$s} AS struct, MAP {'k': 'ok'::%1$s} AS map,
                   [{'inner': ['ok'::%1$s]}] AS deep
            """;
    private static final String ENUM_TYPE = "ENUM('sad', 'ok', 'happy')";

    private interface Writes {
        void to(ArrowReader reader, ByteArrayOutputStream out) throws IOException;
    }

    private static String write(String sql, Writes writes) throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            return write(conn, sql, writes);
        }
    }

    private static String write(DuckDBConnection conn, String sql, Writes writes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, sql, 1024)) {
            writes.to(reader, out);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * As {@link #write(String, Writes)}, on a fresh in-memory database with {@code setting} run on it
     * first: for a GLOBAL setting, which on the pool's database would leak into every other test.
     */
    private static String writeIsolated(String setting, String sql, Writes writes) throws Exception {
        try (DuckDBConnection conn = (DuckDBConnection) java.sql.DriverManager.getConnection("jdbc:duckdb:");
             var st = conn.createStatement()) {
            st.execute(setting);
            return write(conn, sql, writes);
        }
    }

    // Many batches (5000 rows read 1024 at a time): the dictionaries must hold across them.
    private static final String ENUM_MANY_SQL = """
            SELECT i, (['sad', 'ok', 'happy'])[i %% 3 + 1]::%1$s AS m,
                   [(['sad', 'ok', 'happy'])[i %% 3 + 1]::%1$s] AS l
            FROM range(5000) t(i) ORDER BY i
            """;

    @Test
    void enumWritesItsValuesAsTsvAndJsonl() throws Exception {
        Writes tsv = ResultStreams::writeTsv;
        Writes jsonl = ResultStreams::writeJsonl;
        assertEquals(write(ENUM_SQL.formatted("VARCHAR"), tsv), write(ENUM_SQL.formatted(ENUM_TYPE), tsv));
        assertEquals(write(ENUM_SQL.formatted("VARCHAR"), jsonl), write(ENUM_SQL.formatted(ENUM_TYPE), jsonl));
        assertEquals("{\"top\":\"ok\",\"null_value\":null,\"list\":[\"happy\",null],"
                        + "\"struct\":{\"mood\":\"sad\",\"missing\":null},\"map\":{\"k\":\"ok\"},"
                        + "\"deep\":[{\"inner\":[\"ok\"]}]}\n",
                write(ENUM_SQL.formatted(ENUM_TYPE), jsonl));
    }

    @Test
    void enumRoundTripsThroughArrow() throws Exception {
        // writeArrow must send the dictionaries along; reading back resolves the values.
        Writes arrowThenTsv = (reader, out) -> {
            ByteArrayOutputStream arrow = new ByteArrayOutputStream();
            ResultStreams.writeArrow(reader, arrow, CompressionUtil.CodecType.ZSTD, CommonsCompressionFactory.INSTANCE);
            try (BufferAllocator allocator = new RootAllocator();
                 ArrowReader back = new ArrowStreamReader(new ByteArrayInputStream(arrow.toByteArray()),
                         allocator, CommonsCompressionFactory.INSTANCE)) {
                ResultStreams.writeTsv(back, out);
            }
        };
        assertEquals(write(ENUM_SQL.formatted("VARCHAR"), ResultStreams::writeTsv),
                write(ENUM_SQL.formatted(ENUM_TYPE), arrowThenTsv));
    }

    @Test
    void dictionaryEncodedColumnWithoutItsDictionaryFails() throws Exception {
        // Printing the dictionary indices instead would be silently wrong.
        var failure = assertThrows(IllegalStateException.class, () -> write(ENUM_SQL.formatted(ENUM_TYPE),
                (reader, out) -> {
                    reader.loadNextBatch();
                    ResultStreams.writeTsvRows(reader.getVectorSchemaRoot(), new StringWriter());
                }));
        assertTrue(failure.getMessage().contains("'top' is dictionary-encoded"), failure.getMessage());
    }

    @Test
    void enumAcrossManyBatchesInEveryFormat() throws Exception {
        Writes tsv = ResultStreams::writeTsv;
        Writes jsonl = ResultStreams::writeJsonl;
        Writes arrowThenTsv = (reader, out) -> {
            ByteArrayOutputStream arrow = new ByteArrayOutputStream();
            ResultStreams.writeArrow(reader, arrow, CompressionUtil.CodecType.ZSTD, CommonsCompressionFactory.INSTANCE);
            try (BufferAllocator allocator = new RootAllocator();
                 ArrowReader back = new ArrowStreamReader(new ByteArrayInputStream(arrow.toByteArray()),
                         allocator, CommonsCompressionFactory.INSTANCE)) {
                ResultStreams.writeTsv(back, out);
            }
        };
        String varchar = ENUM_MANY_SQL.formatted("VARCHAR");
        String asEnum = ENUM_MANY_SQL.formatted(ENUM_TYPE);
        assertEquals(write(varchar, tsv), write(asEnum, tsv));
        assertEquals(write(varchar, jsonl), write(asEnum, jsonl));
        assertEquals(write(varchar, tsv), write(asEnum, arrowThenTsv));
    }

    @Test
    void writeArrowSendsEachDictionaryOnceBeforeTheFirstBatch() throws Exception {
        // ArrowStreamWriter writes the dictionaries with the first batch, once it is loaded, and
        // again only if they change; DuckDB's ENUM dictionary does not change between batches.
        int[] counts = new int[3]; // dictionary batches, record batches, dictionaries after a record batch
        write(ENUM_MANY_SQL.formatted(ENUM_TYPE), (reader, out) -> {
            ByteArrayOutputStream arrow = new ByteArrayOutputStream();
            ResultStreams.writeArrow(reader, arrow, CompressionUtil.CodecType.NO_COMPRESSION, null);
            try (BufferAllocator allocator = new RootAllocator();
                 var messages = new MessageChannelReader(new ReadChannel(
                         Channels.newChannel(new ByteArrayInputStream(arrow.toByteArray()))), allocator)) {
                MessageResult message;
                while ((message = messages.readNext()) != null) {
                    if (message.getMessage().headerType() == MessageHeader.DictionaryBatch) {
                        counts[0]++;
                        if (counts[1] > 0) {
                            counts[2]++;
                        }
                    } else if (message.getMessage().headerType() == MessageHeader.RecordBatch) {
                        counts[1]++;
                    }
                    if (message.getBodyBuffer() != null) {
                        message.getBodyBuffer().close();
                    }
                }
            }
        });
        assertTrue(counts[1] > 1, "several record batches: " + counts[1]);
        assertEquals(2, counts[0], "one dictionary per ENUM column (m and l's elements), written once");
        assertEquals(0, counts[2], "no dictionary written after the first record batch");
    }

    @Test
    void enumInsideALargeList() throws Exception {
        // arrow_large_buffer_size makes DuckDB send LargeList (and LargeUtf8) instead. It is a
        // GLOBAL setting, hence a database of its own.
        String large = "SET arrow_large_buffer_size = true";
        String sql = "SELECT ['ok'::%1$s, NULL] AS l, 'sad'::%1$s AS m FROM range(3)";
        writeIsolated(large, sql.formatted(ENUM_TYPE), (reader, out) -> assertEquals(ArrowType.ArrowTypeID.LargeList,
                reader.getVectorSchemaRoot().getSchema().findField("l").getType().getTypeID()));
        Writes tsv = ResultStreams::writeTsv;
        Writes jsonl = ResultStreams::writeJsonl;
        assertEquals(writeIsolated(large, sql.formatted("VARCHAR"), tsv), writeIsolated(large, sql.formatted(ENUM_TYPE), tsv));
        assertEquals(writeIsolated(large, sql.formatted("VARCHAR"), jsonl), writeIsolated(large, sql.formatted(ENUM_TYPE), jsonl));
        String enumJsonl = writeIsolated(large, sql.formatted(ENUM_TYPE), jsonl);
        assertTrue(enumJsonl.startsWith("{\"l\":[\"ok\",null],\"m\":\"sad\"}\n"), enumJsonl);
    }
}
