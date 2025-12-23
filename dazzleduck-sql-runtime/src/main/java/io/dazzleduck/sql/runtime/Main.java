package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.ConfigBasedStartupScriptProvider;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.FlightSqlProducerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;

public class Main {
    public static void main(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(config);
    }

    public static void start(Config config) throws Exception {
        String warehousePath = ConfigUtils.getWarehousePath(config);
        createWarehouse(warehousePath);

        // Execute startup script if provided
        StartupScriptProvider startupScriptProvider = ConfigBasedProvider.load(config,
                StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
        if (startupScriptProvider.getStartupScript() != null) {
            ConnectionPool.execute(startupScriptProvider.getStartupScript());
        }

        var networkingMode = config.getStringList("networking_modes");

        // Create shared producer instance if any networking mode is enabled
        DuckDBFlightSqlProducer producer = null;
        BufferAllocator allocator = null;

        if (networkingMode.contains("http") || networkingMode.contains("flight-sql")) {
            // Create single shared allocator and producer
            allocator = new RootAllocator();
            producer = FlightSqlProducerFactory.builder(config)
                    .withAllocator(allocator)
                    .build();
            System.out.println("Created shared FlightSqlProducer for networking services");
        }

        if (networkingMode.contains("http")) {
            io.dazzleduck.sql.http.server.Main.start(config, producer, allocator);
        }
        if (networkingMode.contains("flight-sql")) {
            io.dazzleduck.sql.flight.server.Main.start(config, producer, allocator);
        }
    }


    private static void createWarehouse(String path) throws IOException {
        var p = Path.of(path);
        if(!Files.exists(p)) {
            Files.createDirectories(p);
        }
        System.out.println("Warehouse Path :" + p);
    }
}
