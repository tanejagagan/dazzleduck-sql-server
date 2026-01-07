package io.dazzleduck.sql.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            printBanner();
            Runtime runtime = Runtime.start(args);

            java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received");
                runtime.close();
            }, "shutdown-hook"));
        } catch (com.typesafe.config.ConfigException.Missing e) {
            logger.error("Missing required configuration: {}", e.getMessage());
            System.err.println("ERROR: Missing required configuration: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Failed to start DazzleDuck Runtime", e);
            System.err.println("ERROR: Failed to start DazzleDuck Runtime: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printBanner() {
        String version = Main.class.getPackage().getImplementationVersion();
        logger.info("=".repeat(60));
        logger.info("DazzleDuck Runtime {}", version != null ? "v" + version : "(development)");
        logger.info("=".repeat(60));
    }
}
