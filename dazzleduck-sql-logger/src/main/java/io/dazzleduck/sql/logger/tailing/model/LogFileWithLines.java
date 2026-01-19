package io.dazzleduck.sql.logger.tailing.model;

import java.util.List;
import java.util.Objects;

/**
 * Container for log lines read from a specific file
 */
public final class LogFileWithLines {
    private final String fileName;
    private final List<String> lines;

    public LogFileWithLines(String fileName, List<String> lines) {
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("fileName cannot be null or empty");
        }
        if (lines == null) {
            throw new IllegalArgumentException("lines cannot be null");
        }
        this.fileName = fileName;
        this.lines = lines;
    }

    public String fileName() {
        return fileName;
    }

    public List<String> lines() {
        return lines;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogFileWithLines that = (LogFileWithLines) o;
        return Objects.equals(fileName, that.fileName) &&
               Objects.equals(lines, that.lines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, lines);
    }

    @Override
    public String toString() {
        return "LogFileWithLines[fileName=" + fileName + ", lines=" + lines + "]";
    }
}
