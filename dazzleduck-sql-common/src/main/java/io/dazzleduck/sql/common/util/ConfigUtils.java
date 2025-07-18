package io.dazzleduck.sql.common.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.List;

public class ConfigUtils {

    public record ConfigWithMainParameters(Config config, List<String> mainParameters){}

    public static ConfigWithMainParameters loadCommandLineConfig(String[] args) {
        var argv = new Args();
        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var buffer = new StringBuilder();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                buffer.append(c);
                buffer.append("\n");
            });
        }

        return new ConfigWithMainParameters(ConfigFactory.parseString(buffer.toString()), argv.mainParameters);
    }

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;

        @Parameter
        private List<String>  mainParameters;
    }
}
