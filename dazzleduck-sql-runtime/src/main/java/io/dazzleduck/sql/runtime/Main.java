package io.dazzleduck.sql.runtime;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;

public class Main {


    public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));

        var networkingMode = config.getStringList("networking_modes");

        if (networkingMode.contains("http")) {
            io.dazzleduck.sql.http.server.Main.main(args);
        }
        if (networkingMode.contains("flight-sql")) {
            io.dazzleduck.sql.flight.server.Main.main(args);
        }
    }
}
