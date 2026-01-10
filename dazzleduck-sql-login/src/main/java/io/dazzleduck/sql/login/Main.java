package io.dazzleduck.sql.login;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;


public class Main {
    // API versioning
    private static final String API_VERSION = "v1";
    private static final String API_VERSION_PREFIX = "/" + API_VERSION;

    // Endpoints
    private static final String ENDPOINT_LOGIN = API_VERSION_PREFIX + "/login";

    // Configuration keys
    public static final String CONFIG_PATH = "dazzleduck_login_service";
    private static final String CONFIG_HTTP = "http";
    private static final String CONFIG_JWT_EXPIRATION = "jwt_token.expiration";
    private static final String CONFIG_FLIGHT_SQL = "flight_sql";

    // HTTP protocols
    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";
    public static void main(String[] args) throws Exception {

        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config helidonConfig = Config.create();
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        var httpConfig =  appConfig.getConfig(CONFIG_HTTP);
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);
        var secretKey = Validator.fromBase64String(appConfig.getString(ConfigUtils.SECRET_KEY_KEY));
        var jwtExpiration = appConfig.getDuration(CONFIG_JWT_EXPIRATION);
        WebServer server = WebServer.builder()
                .config(helidonConfig.get(ConfigUtils.CONFIG_PATH))
                .config(helidonConfig.get(CONFIG_FLIGHT_SQL))
                .routing(routing -> {
                    var b = routing
                            .register(ENDPOINT_LOGIN, new LoginService(appConfig, secretKey, jwtExpiration));
                })
                .port(port)
                .host(host)
                .build()
                .start();
        var http = server.hasTls() ? PROTOCOL_HTTPS : PROTOCOL_HTTP;
        String url = "%s://%s:%s".formatted(http, host, server.port());
        System.out.println("Http Server is up: Listening on URL: " + url);
        System.out.println("Login endpoint: " + url + ENDPOINT_LOGIN);
    }
}
