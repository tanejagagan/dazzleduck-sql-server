package io.dazzleduck.sql.flight.server;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a TSV (tab-separated values) InputStream into an Arrow IPC InputStream.
 *
 * <p>The first line of the TSV is treated as the header row (column names).
 * All columns are typed as VARCHAR. Null/empty TSV fields become null Arrow values.
 *
 * <p>The returned InputStream contains a complete Arrow IPC stream that can be fed
 * directly into the existing Arrow-based ingestion pipeline.
 */
public class TsvToArrowConverter {

    private static final int BATCH_SIZE = 10_000;

    /**
     * Converts TSV input to an Arrow IPC stream.
     *
     * @param tsvInput the TSV input stream (first line must be headers)
     * @return an InputStream containing Arrow IPC format data, or an empty stream if input is empty
     * @throws IOException if reading the TSV or writing the Arrow stream fails
     */
    public static InputStream convert(InputStream tsvInput) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(tsvInput, StandardCharsets.UTF_8));

        String headerLine = reader.readLine();
        if (headerLine == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        String[] columnNames = headerLine.split("\t", -1);
        List<Field> fields = new ArrayList<>();
        for (String name : columnNames) {
            fields.add(new Field(name.trim(), FieldType.nullable(new ArrowType.Utf8()), null));
        }
        Schema schema = new Schema(fields);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {

            writer.start();

            List<String[]> batch = new ArrayList<>(BATCH_SIZE);
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    batch.add(line.split("\t", -1));
                    if (batch.size() >= BATCH_SIZE) {
                        writeBatch(root, batch, columnNames.length);
                        writer.writeBatch();
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                writeBatch(root, batch, columnNames.length);
                writer.writeBatch();
            }

            writer.end();
        }

        return new ByteArrayInputStream(baos.toByteArray());
    }

    private static void writeBatch(VectorSchemaRoot root, List<String[]> rows, int columnCount) {
        root.allocateNew();
        for (int row = 0; row < rows.size(); row++) {
            String[] fields = rows.get(row);
            for (int col = 0; col < columnCount; col++) {
                VarCharVector vector = (VarCharVector) root.getVector(col);
                if (col < fields.length && fields[col] != null && !fields[col].isEmpty()) {
                    vector.setSafe(row, new Text(fields[col]));
                } else {
                    vector.setNull(row);
                }
            }
        }
        root.setRowCount(rows.size());
    }
}
