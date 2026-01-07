package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            printBanner();
            var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
            var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
            start(config);
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

    public static void start(Config config) throws Exception {
        Runtime runtime = Runtime.start(config);

        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            runtime.close();
        }, "shutdown-hook"));
    }
}
