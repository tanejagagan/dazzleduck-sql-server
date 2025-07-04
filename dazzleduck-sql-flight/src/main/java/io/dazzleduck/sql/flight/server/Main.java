package io.dazzleduck.sql.flight.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;



public class Main {

    public static final String CONFIG_PATH = "dazzleduck-flight-server";

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Args argv = new Args();
        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var configMap = new HashMap<String, String>();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                var e = c.indexOf("=");
                var key = c.substring(0, e);
                var value = c.substring(e, c.length() - 1);
                configMap.put(key, value);
            });
        }

        var commandlineConfig = ConfigFactory.parseMap(configMap);
        var config = commandlineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        int port = config.getInt("port");
        String host = config.getString("host");
        CallHeaderAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        Location location = Location.forGrpcTls(host, port);
        String keystoreLocation = config.getString("keystore");
        String serverCertLocation = config.getString("serverCert");
        String warehousePath = config.hasPath("warehousePath") ? config.getString("warehousePath") : System.getProperty("user.dir") + "/warehouse";
        String secretKey = config.getString("secretKey");
        String producerId = config.hasPath("producerId") ? config.getString("producerId") : UUID.randomUUID().toString();
        if(!checkWarehousePath(warehousePath)) {
            System.out.printf("Warehouse dir does not exist %s. Create the dir to proceed", warehousePath);
        }
        AccessMode accessMode = config.hasPath("accessMode") ? AccessMode.valueOf(config.getString("accessNode")) : AccessMode.COMPLETE;
        BufferAllocator allocator = new RootAllocator();
        var producer = new DuckDBFlightSqlProducer(location, producerId, secretKey, allocator, warehousePath, accessMode, new NOOPAuthorizer());
        var certStream =  getInputStreamForResource(serverCertLocation);
        var keyStream = getInputStreamForResource(keystoreLocation);
        FlightServer flightServer = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(authenticator)
                .useTls(certStream, keyStream )
                .build();

        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
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
