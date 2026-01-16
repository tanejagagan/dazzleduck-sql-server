package io.dazzleduck.sql.flight;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConfigBasedProvider;

import java.io.IOException;

public interface StartupScriptProvider extends ConfigBasedProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup_script_provider";
    void setConfig(Config config);

    String getStartupScript()  throws IOException;

    static StartupScriptProvider load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
    }
}
