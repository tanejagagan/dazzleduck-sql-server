
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import org.apache.arrow.memory.RootAllocator;

import java.security.NoSuchAlgorithmException;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {


    /**
     * Cannot be instantiated.
     */
    private Main() {
    }



    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws NoSuchAlgorithmException {
        
        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config helidonConfig = Config.create();
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        var port = appConfig.getInt("http.port");
        var host = appConfig.getString("http.host");
        var auth = appConfig.hasPath("auth") ? appConfig.getString("auth") : null;
        String warehousePath = appConfig.hasPath("warehousePath") ?
                appConfig.getString("warehousePath") : System.getProperty("user.dir") + "/warehouse";
        var secretKey = Validator.generateRandoSecretKey();
        var allocator = new RootAllocator();
        String location = "http://%s:%s".formatted(host, port);
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    var b = routing.register("/query", new QueryService(allocator))
                            .register("/login", new LoginService(appConfig, secretKey))
                            .register("/plan", new PlaningService(location, allocator))
                            .register("/ingest", new IngestionService(warehousePath, allocator));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter("/query", appConfig, secretKey));
                    }
                })
                .port(port)
                .build()
                .start();
        String url = "http://localhost:" + server.port();
        System.out.println("Flight Server is up: Listening on URL: " + url);
    }
}