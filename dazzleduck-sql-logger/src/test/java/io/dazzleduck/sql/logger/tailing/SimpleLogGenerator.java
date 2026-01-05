package io.dazzleduck.sql.logger.tailing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public final class SimpleLogGenerator {

    public static void main(String[] args) throws Exception {
        if (args.length >= 3) {
            // multi-file mode
            Path logDir = Path.of(args[0]);
            int numFiles = Integer.parseInt(args[1]);
            int logsPerFile = Integer.parseInt(args[2]);

            Files.createDirectories(logDir);
            Files.createDirectories(logDir);

            for (int fileIndex = 1; fileIndex <= numFiles; fileIndex++) {
                Path logFile = logDir.resolve("app-" + fileIndex + ".log");
                // Tell Logback where to write
                System.setProperty("LOG_FILE", logFile.toAbsolutePath().toString());
                Logger logger = LoggerFactory.getLogger("App-" + fileIndex);
                for (int i = 1; i <= logsPerFile; i++) {
                    logger.info("log-{}", i);
                }
            }
        } else if (args.length >= 1) {
            // single-file mode
            Path logFile = Path.of(args[0]);
            Files.createDirectories(logFile.getParent());
            System.setProperty("LOG_FILE", logFile.toAbsolutePath().toString());
            Logger logger = LoggerFactory.getLogger(SimpleLogGenerator.class);
            logger.info("Single log entry at {}", Instant.now());
        } else {
            System.err.println("Usage:");
            System.err.println("  java SimpleLogGenerator <filePath>");
            System.err.println("  java SimpleLogGenerator <logDir> <numFiles> <logsPerFile>");
            System.exit(1);
        }
    }
}