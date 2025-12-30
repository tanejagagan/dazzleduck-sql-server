package io.dazzleduck.sql.client.tailing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.client.tailing.model.LogMessage;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Converts JSON log messages to Apache Arrow format
 */
public final class JsonToArrowConverter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(JsonToArrowConverter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAllocator allocator;
    private final Schema schema;

    public JsonToArrowConverter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.schema = createArrowSchema();
    }

    /**
     * Get the Arrow schema for log messages
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Convert list of JSON log lines to Arrow VectorSchemaRoot
     */
    public VectorSchemaRoot convertToArrow(List<String> jsonLines) {
        if (jsonLines == null || jsonLines.isEmpty()) {
            return null;
        }

        List<LogMessage> logMessages = parseJsonLines(jsonLines);

        if (logMessages.isEmpty()) {
            logger.warn("No valid log messages parsed from {} lines", jsonLines.size());
            return null;
        }

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.setRowCount(logMessages.size());
        populateVectors(root, logMessages);
        return root;
    }

    private List<LogMessage> parseJsonLines(List<String> jsonLines) {
        List<LogMessage> logMessages = new ArrayList<>();

        for (String jsonLine : jsonLines) {
            try {
                LogMessage log = MAPPER.readValue(jsonLine, LogMessage.class);
                logMessages.add(log);
            } catch (IOException e) {
                logger.error("Failed to parse JSON line: {}", jsonLine, e);
            }
        }

        return logMessages;
    }

    private Schema createArrowSchema() {
        return new Schema(Arrays.asList(
                new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    private void populateVectors(VectorSchemaRoot root, List<LogMessage> logMessages) {
        VarCharVector timestampVec = (VarCharVector) root.getVector("timestamp");
        VarCharVector levelVec = (VarCharVector) root.getVector("level");
        VarCharVector threadVec = (VarCharVector) root.getVector("thread");
        VarCharVector loggerVec = (VarCharVector) root.getVector("logger");
        VarCharVector messageVec = (VarCharVector) root.getVector("message");

        for (int i = 0; i < logMessages.size(); i++) {
            LogMessage log = logMessages.get(i);

            setVectorValue(timestampVec, i, log.timestamp());
            setVectorValue(levelVec, i, log.level());
            setVectorValue(threadVec, i, log.thread());
            setVectorValue(loggerVec, i, log.logger());
            setVectorValue(messageVec, i, log.message());
        }
    }

    private void setVectorValue(VarCharVector vector, int index, String value) {
        if (value != null) {
            vector.setSafe(index, value.getBytes());
        } else {
            vector.setNull(index);
        }
    }

    @Override
    public void close() {
        allocator.close();
    }
}