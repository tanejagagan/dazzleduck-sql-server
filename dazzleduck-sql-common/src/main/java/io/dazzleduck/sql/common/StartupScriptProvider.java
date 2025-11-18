package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;

public interface StartupScriptProvider extends ConfigBasedProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup_script_provider";
    void setConfig(Config config);

    String getStartupScript()  throws IOException;

    static StartupScriptProvider load(Config config) throws Exception {
        return (StartupScriptProvider) ConfigBasedProvider.load(config, STARTUP_SCRIPT_CONFIG_PREFIX,
                new ConfigBasedStartupScriptProvider());
    }
}
