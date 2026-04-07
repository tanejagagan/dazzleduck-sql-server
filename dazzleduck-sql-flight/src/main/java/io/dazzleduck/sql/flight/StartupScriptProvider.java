package io.dazzleduck.sql.flight;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface StartupScriptProvider extends ConfigBasedProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup_script_provider";
    Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

    void setConfig(Config config);

    String getStartupScript()  throws IOException;

    static StartupScriptProvider load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
    }

    /**
     * Replaces all environment variable references in the given content with their actual values
     * from the process environment ({@link System#getenv}).
     *
     * <p>Supported syntax: {@code ${VAR_NAME}} where {@code VAR_NAME} must start with a letter or
     * underscore and contain only letters, digits, or underscores (i.e. matches
     * {@code [A-Za-z_][A-Za-z0-9_]*}).
     *
     * <p>All referenced variables must be present in the environment. If any are missing, an
     * {@link IllegalArgumentException} is thrown listing every unresolved variable so the caller
     * can fix the misconfiguration before the script reaches DuckDB.
     *
     * <p>Example:
     * <pre>{@code
     * // Given: ENV WAREHOUSE=/data
     * String script = "ATTACH '${WAREHOUSE}/db.duckdb' AS db;";
     * String result = StartupScriptProvider.replaceEnvVariable(script);
     * // result => "ATTACH '/data/db.duckdb' AS db;"
     * }</pre>
     *
     * @param content the script or configuration text containing {@code ${VAR_NAME}} references
     * @return the content with all environment variable references substituted
     * @throws IllegalArgumentException if one or more referenced environment variables are not set
     */
    static String replaceEnvVariable(String content) {
        Matcher matcher = ENV_VAR_PATTERN.matcher(content);
        StringBuilder result = new StringBuilder();
        List<String> missing = new ArrayList<>();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String value = System.getenv(varName);
            if (value == null) {
                missing.add(varName);
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(value));
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Startup script references undefined environment variable(s): " + missing);
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
