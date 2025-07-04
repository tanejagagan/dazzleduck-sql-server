
package io.dazzleduck.sql.http.server;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import org.apache.arrow.memory.RootAllocator;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;


/**
 * The application main class.
 */
public class Main {


    private static final String CONFIG_PATH = "dazzleduck-http-server";

    /**
     * Cannot be instantiated.
     */
    private Main() {
    }

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;
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

        var argv = new Args();

        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var configMap = new HashMap<String, String>();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                var e = c.split("=");
                var key = e[0];
                var value = e[1];
                configMap.put(key, value);
            });
        }

        var commandlineConfig = ConfigFactory.parseMap(configMap);
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load().getConfig(Main.CONFIG_PATH));
        var port = Integer.parseInt(appConfig.getString("port"));
        var auth = appConfig.hasPath("auth") ? appConfig.getString("auth") : null;
        var secretKey = Validator.generateRandoSecretKey();
        var allocator = new RootAllocator();
        String location = "http://localhost:" + port;
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    var b = routing.register("/query", new QueryService(allocator))
                            .register("/login", new LoginService(appConfig, secretKey))
                            .register("/plan", new PlaningService(location, allocator));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter("/query", appConfig, secretKey));
                    }
                })
                .port(port)
                .build()
                .start();

        var builder = new StringBuilder();
        String url = "http://localhost:" + server.port();
        String msg = "WEB server is up! " + url;
        builder.append(msg);
        System.out.println(builder.toString());
    }
}