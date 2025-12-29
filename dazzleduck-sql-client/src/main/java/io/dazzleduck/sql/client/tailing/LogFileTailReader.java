package io.dazzleduck.sql.client.tailing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Continuously reads new lines from a log file, tracking byte position.
 * Only reads newly appended data since last read.
 */
public final class LogFileTailReader {

    private static final Logger logger = LoggerFactory.getLogger(LogFileTailReader.class);

    private final Path logFilePath;
    private long lastReadPosition;

    public LogFileTailReader(String logFilePath) {
        this.logFilePath = Paths.get(logFilePath);
        this.lastReadPosition = 0;
    }

    /**
     * Reads new lines appended since last read.
     * @return List of new JSON log lines
     */
    public List<String> readNewLines() throws IOException {
        List<String> newLines = new ArrayList<>();

        if (!Files.exists(logFilePath)) {
            logger.debug("Log file not found: {}", logFilePath);
            return newLines;
        }

        try (RandomAccessFile raf = new RandomAccessFile(logFilePath.toFile(), "r")) {
            long currentFileLength = raf.length();

            // Handle file truncation or rotation
            if (currentFileLength < lastReadPosition) {
                logger.warn("File truncated or rotated. Resetting position from {} to 0", lastReadPosition);
                lastReadPosition = 0;
            }

            // Read only if there's new data
            if (currentFileLength > lastReadPosition) {
                long bytesToRead = currentFileLength - lastReadPosition;
                logger.debug("Reading bytes {} to {} ({} bytes)",
                        lastReadPosition, currentFileLength, bytesToRead);

                raf.seek(lastReadPosition);

                String line;
                while ((line = raf.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        newLines.add(line);
                    }
                }

                lastReadPosition = raf.getFilePointer();
            }
        }

        return newLines;
    }

    public long getLastReadPosition() {
        return lastReadPosition;
    }

    public void reset() {
        lastReadPosition = 0;
        logger.info("Reader position reset to 0");
    }
}