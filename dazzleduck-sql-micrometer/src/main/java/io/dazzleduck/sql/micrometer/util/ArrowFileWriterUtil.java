package io.dazzleduck.sql.micrometer.util;

import io.dazzleduck.sql.commons.types.JavaRow;
import io.dazzleduck.sql.commons.types.VectorSchemaRootWriter;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to export Micrometer Meters into Apache Arrow format.
 * This version uses {@link VectorSchemaRootWriter} and {@link JavaRow}
 * for schema-driven, type-safe writing.
 */
public final class ArrowFileWriterUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFileWriterUtil.class);

    private ArrowFileWriterUtil() {
    }

    // --- PUBLIC APIs ----------------------------------------------------------

    /**
     * Writes meters to an Arrow IPC file on disk
     */
    public static void writeMetersToFile(List<Meter> meters, String filePath) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             FileOutputStream fos = new FileOutputStream(filePath)) {
            writeMeters(allocator, meters, fos);
        }
    }

    /**
     * Converts meters into Arrow IPC bytes in-memory
     */
    public static byte[] convertMetersToArrowBytes(List<Meter> meters) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeMeters(allocator, meters, out);
            return out.toByteArray();
        }
    }

    // --- INTERNAL LOGIC -------------------------------------------------------

    private static void writeMeters(BufferAllocator allocator, List<Meter> meters, OutputStream os) throws IOException {
        Schema schema = createMeterSchema();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, os)) {
            // Create schema-driven writer
            var vectorWriter = VectorSchemaRootWriter.of(schema);
            writer.start();
            // Deterministic order for reproducibility
            List<Meter> sortedMeters = meters.stream()
                    .sorted(Comparator.comparing((Meter m) -> m.getId().getType().name())
                            .thenComparing(m -> m.getId().getName()))
                    .toList();
            List<JavaRow> rows = new ArrayList<>(sortedMeters.size());
            for (Meter meter : sortedMeters) {
                Meter.Id id = meter.getId();
                Map<String, String> tagsMap = new LinkedHashMap<>();
                for (Tag t : id.getTags()) {
                    tagsMap.put(t.getKey(), t.getValue());
                }
                double value = Double.NaN, min = Double.NaN, max = Double.NaN, mean = Double.NaN;
                // --- Handle all meter types ---
                try {
                    switch (meter) {
                        case Counter c -> value = c.count();
                        case Gauge g -> value = g.value();
                        case Timer tmr -> {
                            HistogramSnapshot snap = tmr.takeSnapshot();
                            value = snap.total(TimeUnit.SECONDS);
                            double[] vals = Arrays.stream(snap.percentileValues())
                                    .mapToDouble(v -> v.value(TimeUnit.SECONDS)).toArray();
                            if (vals.length > 0) {
                                min = Arrays.stream(vals).min().orElse(Double.NaN);
                                max = Arrays.stream(vals).max().orElse(Double.NaN);
                                mean = Arrays.stream(vals).average().orElse(Double.NaN);
                            }
                        }
                        case DistributionSummary ds -> {
                            value = ds.count();
                            min = 0.0;
                            max = ds.max();
                            mean = ds.mean();
                        }
                        case LongTaskTimer ltt -> {
                            value = ltt.activeTasks();
                            double totalTime = ltt.duration(TimeUnit.SECONDS);
                            max = totalTime;
                            if (value > 0) mean = totalTime / value;
                        }
                        case FunctionCounter fc -> value = fc.count();
                        case FunctionTimer ft -> {
                            double count = ft.count();
                            double totalTime = ft.totalTime(TimeUnit.SECONDS);
                            value = count;
                            min = 0.0;
                            max = totalTime;
                            if (count > 0) mean = totalTime / count;
                        }
                        default -> {
                            // Generic fallback
                            Iterable<Measurement> measurements = meter.measure();
                            double total = 0;
                            int cnt = 0;
                            for (Measurement m : measurements) {
                                total += m.getValue();
                                cnt++;
                            }
                            if (cnt > 0) {
                                value = total;
                                mean = total / cnt;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error collecting value for meter: {}", id.getName(), e);
                }
                // --- Prepare row ---
                var row = new JavaRow(new Object[]{
                        id.getName(),
                        id.getType().name().toLowerCase(),
                        tagsMap,
                        !Double.isNaN(value) ? value : null,
                        !Double.isNaN(min) ? min : null,
                        !Double.isNaN(max) ? max : null,
                        !Double.isNaN(mean) ? mean : null
                });
                rows.add(row);
            }
            // --- Write all rows to Arrow vectors ---
            vectorWriter.writeToVector(rows.toArray(JavaRow[]::new), root);
            // --- Flush & close ---
            writer.writeBatch();
            writer.end();
        }
    }

    // --- SCHEMA DEFINITION ----------------------------------------------------

    private static Schema createMeterSchema() {
        List<Field> schemaFields = new ArrayList<>();
        schemaFields.add(new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null));
        schemaFields.add(new Field("type", FieldType.notNullable(new ArrowType.Utf8()), null));
        // Map<String, String> field: tags
        Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field valField = new Field("value", FieldType.nullable(new ArrowType.Utf8()), null);
        Field entriesStruct = new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                Arrays.asList(keyField, valField));
        Field tagsField = new Field("tags", FieldType.notNullable(new ArrowType.Map(false)),
                Collections.singletonList(entriesStruct));
        schemaFields.add(tagsField);
        // Numeric fields
        schemaFields.add(new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("min", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("max", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        schemaFields.add(new Field("mean", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        return new Schema(schemaFields);
    }
}