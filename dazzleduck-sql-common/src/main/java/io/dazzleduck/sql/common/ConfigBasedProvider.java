package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

public interface ConfigBasedProvider {

    String CLASS_KEY = "class";

    static ConfigBasedProvider load(Config config, String prefixKey, ConfigBasedProvider defaultObject) throws Exception {
        var innerConfig = config.getConfig(prefixKey);
        if (innerConfig.hasPath(CLASS_KEY)) {
            var clazz = innerConfig.getString(CLASS_KEY);
            var constructor = Class.forName(clazz).getConstructor();
            ConfigBasedProvider object = (ConfigBasedProvider) constructor.newInstance();
            object.setConfig(innerConfig);
            return object;
        } else {
            return returnDefault(defaultObject, innerConfig);
        }
    }

    void setConfig(Config config);

    private static ConfigBasedProvider returnDefault(ConfigBasedProvider defaultObject,  Config config) {
        defaultObject.setConfig(config);
        return defaultObject;
    }
}
