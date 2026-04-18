package io.dazzleduck.sql.examples;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

class ComposeFiles {

    private ComposeFiles() {}

    /**
     * Returns a temp copy of the compose file with {@code container_name} lines removed
     * so Testcontainers can manage naming without conflicts across concurrent runs.
     */
    static File stripped(File original) throws IOException {
        // Must be under the project tree — macOS Docker Desktop cannot mount system temp dirs
        Path dir = Path.of("target/compose-stripped");
        Files.createDirectories(dir);
        Path out = dir.resolve(original.getParentFile().getName() + "-docker-compose.yml");
        Files.write(out, Files.lines(original.toPath())
                .filter(line -> !line.stripLeading().startsWith("container_name"))
                .collect(Collectors.toList()));
        return out.toFile();
    }
}
