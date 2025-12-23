
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.login.LoginService;
import io.dazzleduck.sql.login.ProxyLoginService;
import io.helidon.config.Config;
import io.helidon.cors.CrossOriginConfig;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.cors.CorsSupport;
import io.helidon.webserver.http.HttpService;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {





    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws Exception {
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(appConfig);
    }

    public static void start(com.typesafe.config.Config appConfig) throws Exception {
        // Create allocator and producer
        var allocator = new RootAllocator();
        String warehousePath = ConfigUtils.getWarehousePath(appConfig);
        String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
        var tempWriteDir = DuckDBFlightSqlProducer.getTempWriteDir(appConfig);
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);

        var httpConfig = appConfig.getConfig("http");
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);

        // Create producer using factory with custom settings for HTTP server
        var producer = io.dazzleduck.sql.flight.server.FlightSqlProducerFactory.builder(appConfig)
                .withLocation(Location.forGrpcInsecure(host, port))
                .withProducerId(UUID.randomUUID().toString())
                .withSecretKey(base64SecretKey)
                .withAllocator(allocator)
                .withWarehousePath(warehousePath)
                .withTempWriteDir(tempWriteDir)
                .withAccessMode(accessMode)
                .build();

        start(appConfig, producer, allocator);
    }

    /**
     * Starts the HTTP server with a pre-existing producer instance.
     * This allows sharing the same producer between multiple servers.
     *
     * @param appConfig the configuration
     * @param producer the FlightSqlProducer instance to use
     * @param allocator the BufferAllocator associated with the producer
     * @throws Exception if server startup fails
     */
    public static void start(com.typesafe.config.Config appConfig, DuckDBFlightSqlProducer producer, org.apache.arrow.memory.BufferAllocator allocator) throws Exception {
        LogConfig.configureRuntime();
        Config helidonConfig = Config.create();
        var httpConfig =  appConfig.getConfig("http");
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);
        var auth = httpConfig.hasPath(ConfigUtils.AUTHENTICATION_KEY) ? httpConfig.getString(ConfigUtils.AUTHENTICATION_KEY) : "none";
        String warehousePath = ConfigUtils.getWarehousePath(appConfig);
        String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64SecretKey);
        String location = "http://%s:%s".formatted(host, port);
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);
        var jwtExpiration = appConfig.getDuration("jwt_token.expiration");
        var cors = CorsSupport.builder()
                .addCrossOrigin(CrossOriginConfig.builder()
                        .allowOrigins(appConfig.hasPath("allow-origin") ? appConfig.getString("allow-origin") : "*")
                        .allowMethods("GET", "POST")
                        .allowHeaders("Content-Type", "Authorization")
                        .build())
                .build();
        HttpService loginService;
        if (appConfig.hasPath(AuthUtils.PROXY_LOGIN_URL_KEY)) {
            var proxyUrl = appConfig.getString(AuthUtils.PROXY_LOGIN_URL_KEY);
            loginService = new ProxyLoginService(proxyUrl);
        } else {
            loginService = new LoginService(appConfig, secretKey, jwtExpiration);
        }

        WebServer server = WebServer.builder()
                .config(helidonConfig.get("dazzleduck_server"))
                .config(helidonConfig.get("flight_sql"))
                .routing(routing -> {
                    routing.register(cors);
                    var b = routing.register("/query", new QueryService(producer, accessMode))
                            .register("/login", loginService)
                            .register("/plan", new PlaningService(producer, location, allocator, accessMode))
                            .register("/cancel", new CancelService(producer, accessMode))
                            .register("/ingest", new IngestionService(producer, warehousePath, allocator))
                            .register("/ui", new UIService(producer));
                    if ("jwt".equals(auth) || accessMode == AccessMode.RESTRICTED) {
                        b.addFilter(new JwtAuthenticationFilter(List.of("/query", "/plan", "/ingest", "/cancel", "/ui"), appConfig, secretKey));
                    }
                })
                .port(port)
                .host(host)
                .build()
                .start();
        var http = server.hasTls() ? "https" : "http";
        String url = "%s://%s:%s".formatted(http, host, port);
        System.out.println("Http Server is up: Listening on URL: " + url);
    }
}