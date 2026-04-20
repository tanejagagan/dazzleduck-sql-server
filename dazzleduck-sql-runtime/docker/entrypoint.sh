#!/bin/sh

# Default JVM options
JVM_OPTS="-Xms512m \
  -XX:+UseContainerSupport \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+ExitOnOutOfMemoryError \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

# Classpath: /app/libs (Jib deps) + /app/extra (plugin modules loaded via extraDirectories)
CLASSPATH="/app/resources:/app/classes:/app/libs/*:/app/extra/*"

# Check if first argument is a fully qualified Java class name
case "$1" in
  io.dazzleduck.*)
    # Running a specific main class (like logger or metric examples)
    MAIN_CLASS=$1
    shift
    echo "Starting $MAIN_CLASS..."
    exec java $JVM_OPTS -Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider -cp $CLASSPATH $MAIN_CLASS "$@"
    ;;
  *)
    # Default: run the runtime Main with all arguments
    echo "Starting DazzleDuck Runtime..."
    exec java $JVM_OPTS -cp $CLASSPATH io.dazzleduck.sql.runtime.Main "$@"
    ;;
esac