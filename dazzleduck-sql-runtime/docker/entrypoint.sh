#!/bin/sh

JVM_OPTS="-Xms512m \
  -XX:+UseContainerSupport \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+ExitOnOutOfMemoryError \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED \
  --sun-misc-unsafe-memory-access=allow \
  -Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider"

CLASSPATH="/app/resources:/app/classes:/app/libs/*:/app/extra/*"

case "$1" in
  io.dazzleduck.*)
    MAIN_CLASS=$1
    shift
    echo "Starting $MAIN_CLASS..."
    exec java $JVM_OPTS -cp $CLASSPATH $MAIN_CLASS "$@"
    ;;
  *)
    echo "Starting DazzleDuck Runtime..."
    exec java $JVM_OPTS -cp $CLASSPATH io.dazzleduck.sql.runtime.Main "$@"
    ;;
esac
