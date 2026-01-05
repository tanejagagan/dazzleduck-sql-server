package io.dazzleduck.sql.login;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.auth.Validator;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.util.Calendar;

public class LoginService implements HttpService {
    // API versioning
    public static final String API_VERSION = "v1";
    public static final String API_VERSION_PREFIX = "/" + API_VERSION;

    // Endpoints
    public static final String ENDPOINT_LOGIN = "/";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(LoginService.class);
    private final Config config;
    private final SecretKey secretKey;
    private final Validator validator;
    private final Duration jwtExpiration;

    public LoginService(Config validatorConfig, SecretKey secretKey, Duration jwtExpiration) {
        this.config = validatorConfig;
        this.validator = getValidator(config);
        this.secretKey = secretKey;
        this.jwtExpiration = jwtExpiration;
    }

    public static Validator getValidator(Config config) {
        return Validator.load(config);
    }
    @Override
    public void routing(HttpRules rules) {
        rules.post(ENDPOINT_LOGIN, this::handleLogin);
    }

    private void handleLogin(ServerRequest serverRequest, ServerResponse serverResponse) throws IOException {
        var inputStream = serverRequest.content().inputStream();
        var loginRequest = MAPPER.readValue(inputStream, LoginRequest.class);
        try {
            logger.debug("Login attempt for user: {}", loginRequest.username());
            validator.validate(loginRequest.username(), loginRequest.password());
            Calendar expiration = Calendar.getInstance();
            expiration.add(Calendar.MINUTE,
                    (int)this.jwtExpiration.toMinutes());
            String jwt = Jwts.builder()
                    .subject(loginRequest.username())
                    .expiration(expiration.getTime())
                    .claims(loginRequest.claims())
                    .signWith(secretKey).compact();
            var response = new LoginResponse(jwt, loginRequest.username());
            MAPPER.writeValue(serverResponse.outputStream(), response);
            logger.info("Login successful for user: {}", loginRequest.username());
        } catch (Exception e ){
            logger.warn("Login failed for user: {}", loginRequest.username(), e);
            serverResponse.status(Status.UNAUTHORIZED_401);
            serverResponse.send();
        }
    }
}