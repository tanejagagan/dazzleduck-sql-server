package io.dazzleduck.sql.logback;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Converts LogEntry objects to Apache Arrow format for efficient transmission.
 */
public final class LogToArrowConverter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogToArrowConverter.class);

    private final BufferAllocator allocator;
    private final Schema schema;

    public LogToArrowConverter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.schema = createArrowSchema();
    }

    /**
     * Get the Arrow schema for log entries.
     *
     * @return the Arrow schema
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Convert list of LogEntry objects to Arrow byte array.
     *
     * @param entries List of log entries to convert
     * @return Arrow stream bytes, or null if entries is empty
     */
    public byte[] convertToArrowBytes(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return null;
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

            root.setRowCount(entries.size());
            populateVectors(root, entries);

            writer.start();
            writer.writeBatch();
            writer.end();

            return out.toByteArray();

        } catch (IOException e) {
            logger.error("Failed to serialize log entries to Arrow format", e);
            throw new RuntimeException("Arrow serialization failed", e);
        }
    }

    /**
     * Convert list of LogEntry objects to VectorSchemaRoot.
     * Caller is responsible for closing the returned root.
     *
     * @param entries List of log entries to convert
     * @return VectorSchemaRoot containing the data, or null if entries is empty
     */
    public VectorSchemaRoot convertToVectorSchemaRoot(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return null;
        }

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.setRowCount(entries.size());
        populateVectors(root, entries);
        return root;
    }

    private Schema createArrowSchema() {
        // MDC field as Map<String, String>
        Field mdcField = new Field("mdc", FieldType.nullable(new ArrowType.Map(false)),
                List.of(
                        new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                                List.of(
                                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                        new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                                )
                        )
                )
        );

        return new Schema(List.of(
                new Field("s_no", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                mdcField
        ));
    }

    private void populateVectors(VectorSchemaRoot root, List<LogEntry> entries) {
        BigIntVector sNoVector = (BigIntVector) root.getVector("s_no");
        TimeStampMilliVector timestampVec = (TimeStampMilliVector) root.getVector("timestamp");
        VarCharVector levelVec = (VarCharVector) root.getVector("level");
        VarCharVector loggerVec = (VarCharVector) root.getVector("logger");
        VarCharVector threadVec = (VarCharVector) root.getVector("thread");
        VarCharVector messageVec = (VarCharVector) root.getVector("message");
        MapVector mdcVec = (MapVector) root.getVector("mdc");

        mdcVec.allocateNew();
        UnionMapWriter mdcWriter = mdcVec.getWriter();

        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);

            sNoVector.setSafe(i, entry.sNo());
            if (entry.timestamp() != null) {
                timestampVec.setSafe(i, entry.timestamp().toEpochMilli());
            } else {
                timestampVec.setNull(i);
            }
            setVectorValue(levelVec, i, entry.level());
            setVectorValue(loggerVec, i, entry.logger());
            setVectorValue(threadVec, i, entry.thread());
            setVectorValue(messageVec, i, entry.message());

            // Write MDC map
            mdcWriter.setPosition(i);
            Map<String, String> mdc = entry.mdc();
            if (mdc != null && !mdc.isEmpty()) {
                mdcWriter.startMap();
                for (Map.Entry<String, String> mdcEntry : mdc.entrySet()) {
                    mdcWriter.startEntry();
                    mdcWriter.key().varChar().writeVarChar(mdcEntry.getKey());
                    if (mdcEntry.getValue() != null) {
                        mdcWriter.value().varChar().writeVarChar(mdcEntry.getValue());
                    } else {
                        mdcWriter.value().writeNull();
                    }
                    mdcWriter.endEntry();
                }
                mdcWriter.endMap();
            } else {
                mdcWriter.writeNull();
            }
        }
        mdcWriter.setValueCount(entries.size());
    }

    private void setVectorValue(VarCharVector vector, int index, String value) {
        if (value != null) {
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vector.setNull(index);
        }
    }

    @Override
    public void close() {
        allocator.close();
    }
}
