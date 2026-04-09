package io.dazzleduck.sql.commons.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Collections;
import java.util.List;

public class CommandLineConfigUtil {

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
        @Parameter(names = {"--conf"}, description = "Configurations", splitter = NoSplitter.class )
        private List<String> configs;

        public static class NoSplitter implements IParameterSplitter {
            @Override
            public List<String> split(String value) {
                return Collections.singletonList(value);
            }
        }

        @Parameter
        private List<String>  mainParameters;
    }
}
