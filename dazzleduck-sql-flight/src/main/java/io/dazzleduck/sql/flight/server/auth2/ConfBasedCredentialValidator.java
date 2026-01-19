package io.dazzleduck.sql.flight.server.auth2;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.common.ConfigConstants;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfBasedCredentialValidator implements AdvanceBasicCallHeaderAuthenticator.AdvanceCredentialValidator {

    private final Map<String, byte[]> userHashMap = new HashMap<>();

    public ConfBasedCredentialValidator(Config config) {
        List<? extends ConfigObject> users = config.getObjectList("users");
        Map<String, String> passwords = new HashMap<>();
        users.forEach(o -> {
            String name = o.toConfig().getString(ConfigConstants.USERNAME_KEY);
            String password = o.toConfig().getString(ConfigConstants.PASSWORD_KEY);
            passwords.put(name, password);
        });
        passwords.forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password, CallHeaders callHeaders) throws Exception {
        var storePassword = userHashMap.get(username);
        if (storePassword != null &&
                !password.isEmpty() &&
                Validator.passwordMatch(storePassword, Validator.hash(password))) {
            return () -> username;
        } else {
            throw new RuntimeException("Authentication failure");
        }
    }
}
