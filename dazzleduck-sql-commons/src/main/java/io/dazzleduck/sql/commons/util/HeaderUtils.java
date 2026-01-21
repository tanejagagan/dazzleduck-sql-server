package io.dazzleduck.sql.commons.util;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class HeaderUtils {

    public static String[] parseCsv(String value) {
        if (value == null || value.isBlank()) {
            return new String[0];
        }
        List<String> result = new ArrayList<>();
        try (CsvReader<CsvRecord> reader = CsvReader.builder().ofCsvRecord(new StringReader(value))) {
            for (CsvRecord record : reader) {
                for (int i = 0; i < record.getFieldCount(); i++) {
                    String field = record.getField(i).trim();
                    if (!field.isEmpty()) {
                        result.add(field);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse CSV value: " + value, e);
        }
        return result.toArray(new String[0]);
    }
}
