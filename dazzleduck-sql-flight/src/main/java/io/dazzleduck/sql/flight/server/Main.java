package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.ConfigBasedStartupScriptProvider;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.io.InputStream;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


public class Main {

    public static void main(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(config);
    }

    public static FlightServer createServer(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        return createServer(config);
    }
    public static FlightServer createServer(Config config) throws Exception {
        AdvanceJWTTokenAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        boolean useEncryption = config.getBoolean("flight_sql.use_encryption");
        String keystoreLocation = config.getString("keystore");
        String serverCertLocation = config.getString("server_cert");
        String warehousePath = ConfigUtils.getWarehousePath(config);

        if(!checkWarehousePath(warehousePath)) {
            System.out.printf("Warehouse dir does not exist %s. Create the dir to proceed", warehousePath);
        }

        // Execute startup script if provided
        StartupScriptProvider startupScriptProvider = ConfigBasedProvider.load(config,
                StartupScriptProvider.STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
        if (startupScriptProvider.getStartupScript() != null) {
            ConnectionPool.execute(startupScriptProvider.getStartupScript());
        }

        // Create allocator and producer using factory
        BufferAllocator allocator = new RootAllocator();
        DuckDBFlightSqlProducer producer = FlightSqlProducerFactory.builder(config)
                .withAllocator(allocator)
                .build();

        var certStream = getInputStreamForResource(serverCertLocation);
        var keyStream = getInputStreamForResource(keystoreLocation);

        var builder = FlightServer.builder(allocator, producer.getLocation(), producer)
                .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                        new AdvanceServerCallHeaderAuthMiddleware.Factory(authenticator));
        if (useEncryption) {
            builder.useTls(certStream, keyStream);
        }
        return builder.build();
    }


    public static void start(Config config) throws Exception {
        var flightServer = createServer(config);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("Flight Server is up: Listening on URI: " + flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
    }




    private static InputStream getInputStreamForResource(String filename) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found! : " + filename);
        }
        return inputStream;
    }

    private static boolean checkWarehousePath(String warehousePath) {
        return true;
    }
}
