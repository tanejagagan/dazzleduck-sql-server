package io.dazzleduck.sql.common;

import com.typesafe.config.Config;


public class ConfigBasedStartupScriptProvider implements StartupScriptProvider {

    private static String CONTENT_KEY = "content";
    private Config config;

    public String getStartupScript() {
        if (config.hasPath(CONTENT_KEY)) {
            return config.getString(CONTENT_KEY);
        }
        return null;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
}
