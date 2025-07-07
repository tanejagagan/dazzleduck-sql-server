package io.dazzleduck.sql.commons.delta;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

/**
 * A utility class for converting values to specific data types.
 */
public class DataTypeConverter {

    /**
     * Convert a value to TINY_INT.
     */
    public static Expression toTinyInt(Object value) {
        return Literal.ofByte(((Number) value).byteValue());
    }

    /**
     * Convert a value to SMALL_INT.
     */
    public static Expression toSmallInt(Object value) {
        return Literal.ofShort(((Number) value).shortValue());
    }

    /**
     * Convert a value to INT.
     */
    public static Expression toInt(Object value) {
        return Literal.ofInt(((Number) value).intValue());
    }

    /**
     * Convert a value to BIG_INT.
     */
    public static Expression toBigInt(Object value) {
        return Literal.ofLong(((Number) value).longValue());
    }

    /**
     * Convert a value to FLOAT.
     */
    public static Expression toFloat(Object value) {
        return Literal.ofFloat(((Number) value).floatValue());
    }

    /**
     * Convert a value to DOUBLE.
     */
    public static Expression toDouble(Object value) {
        return Literal.ofDouble(((Number) value).doubleValue());
    }

    /**
     * Convert a value to DECIMAL with optional precision and scale.
     *
     * Supports:
     * - byte, short, int, long
     * - float, double
     * - java.math.BigDecimal
     * - BOOLEAN (true -> 1, false -> 0)
     * - java.sql.Timestamp (milliseconds since epoch as a decimal)
     *
     * Precision (`p`) and Scale (`s`):
     * - Precision (`p`): Optional, defaults to 10, must be between 1 and 38.
     * - Scale (`s`): Optional, defaults to 0, must be between 0 and `p`.
     *
     * @param value the input value to convert
     * @param p optional maximum precision (1 to 38); defaults to 10 if null
     * @param s optional scale (0 to p); defaults to 0 if null
     * @return a Literal representing the decimal value
     */
    public static Literal toDecimal(Object value, Integer p, Integer s) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null when converting to DECIMAL.");
        }

        // Default precision and scale
        int precision = (p == null) ? 10 : p;
        int scale = (s == null) ? 0 : s;

        // Validate precision and scale
        if (precision < 1 || precision > 38) {
            throw new IllegalArgumentException("Precision must be between 1 and 38. Provided: " + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException("Scale must be between 0 and precision (" + precision + "). Provided: " + scale);
        }

        BigDecimal decimalValue;

        // Handle integral types (byte, short, int, long)
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            decimalValue = BigDecimal.valueOf(((Number) value).longValue());
        }
        // Handle floating-point types (float, double)
        else if (value instanceof Float || value instanceof Double) {
            decimalValue = BigDecimal.valueOf(((Number) value).doubleValue());
        }
        // Handle BigDecimal
        else if (value instanceof BigDecimal) {
            decimalValue = (BigDecimal) value;
        }
        // Handle BOOLEAN (true -> 1, false -> 0)
        else if (value instanceof Boolean) {
            decimalValue = ((Boolean) value) ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        // Handle java.sql.Timestamp (milliseconds since epoch as a decimal)
        else if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            decimalValue = BigDecimal.valueOf(timestamp.getTime());
        }
        // Unsupported input type
        else {
            throw new IllegalArgumentException("Unsupported value type for DECIMAL conversion: " + value.getClass().getName());
        }

        // Ensure the decimal value conforms to the specified precision and scale
        try {
            BigDecimal scaledValue = decimalValue.setScale(scale); // Set the scale
            if (scaledValue.precision() > precision) {
                throw new IllegalArgumentException(
                        String.format("Value exceeds precision (%d). Actual precision: %d, Value: %s", precision, scaledValue.precision(), scaledValue)
                );
            }
            return Literal.ofDecimal(scaledValue, precision, scale);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    String.format("Cannot convert value to DECIMAL with precision=%d and scale=%d: %s", precision, scale, value),
                    e
            );
        }
    }

    /**
     * Convert a value to STRING.
     */
    public static Expression toString(Object value) {
        return Literal.ofString(value.toString());
    }

    /**
     * Convert a value to BINARY.
     */
    public static Expression toBinary(Object value) {
        return Literal.ofBinary((byte[]) value);
    }

    /**
     * Convert a value to BOOLEAN.
     */
    public static Expression toBoolean(Object value) {
        return Literal.ofBoolean((Boolean) value);
    }

    /**
     * Convert a value to TIMESTAMP.
     */
    public static Expression toTimestamp(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null when converting to TIMESTAMP.");
        }
        // Handle NUMERIC (byte, short, int, long, float, double, BigDecimal)
        if (value instanceof Number) {
            long millis = extractMillisFromNumber((Number) value);
            return Literal.ofTimestamp(millis);
        }

        // Handle STRING (parse as ISO-8601 or custom timestamp format)
        if (value instanceof String stringValue) {
            try {
                // Parse ISO-8601 format
                return Literal.ofTimestamp(stringToTimestamp(stringValue, ZoneId.of("UTC")));
            } catch (Exception e) {
                // Handle custom formats if needed
                throw new IllegalArgumentException("Invalid STRING format for TIMESTAMP: " + stringValue, e);
            }
        }

        // DATE Handling (java.sql.Date)
        if (value instanceof java.sql.Date dateValue) {
            long micros = dateValue.getTime() * 1000; // Convert milliseconds to microseconds
            return Literal.ofTimestamp(micros);
        }

        // TIMESTAMP Handling (java.sql.Timestamp)
        if (value instanceof Timestamp timestampValue) {
            long micros = timestampValue.getTime() * 1000 + (timestampValue.getNanos() / 1000 % 1000);
            return Literal.ofTimestamp(micros);
        }

        // TIMESTAMP_NTZ Handling (LocalDateTime, no timezone information)
        if (value instanceof LocalDateTime localDateTime) {
            Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
            long micros = instant.toEpochMilli() * 1000 + (instant.getNano() / 1000 % 1000);
            return Literal.ofTimestamp(micros);
        }

        // BOOLEAN Handling (true -> epoch + 1 microsecond, false -> epoch)
        if (value instanceof Boolean) {
            boolean boolValue = (Boolean) value;
            long micros = boolValue ? 1 : 0;
            return Literal.ofTimestamp(micros);
        }

        // TODO
        // VARIANT Handling (custom logic based on your system's VARIANT type)

        throw new IllegalArgumentException("Unsupported value type for TIMESTAMP conversion: " + value.getClass().getName());
    }



    /**
     * Convert a value to a TIMESTAMP_NTZ (LocalDateTime).
     */
    public static Expression toTimestampNTZ(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null when converting to TIMESTAMP_NTZ.");
        }

        // Handle STRING (parse as ISO-8601 or custom timestamp format)
        if (value instanceof String stringValue) {
            try {
                // Parse ISO-8601 format
                ZoneId localZoneId = ZoneId.systemDefault();
                return Literal.ofTimestamp(stringToTimestamp(stringValue, localZoneId));
            } catch (Exception e) {
                // Handle custom formats if needed
                throw new IllegalArgumentException("Invalid STRING format for TIMESTAMP: " + stringValue, e);
            }
        }

        // DATE Handling (java.sql.Date)
        if (value instanceof java.sql.Date dateValue) {
            long micros = dateValue.getTime() * 1000; // Convert milliseconds to microseconds
            return Literal.ofTimestamp(micros);
        }

        // Handle java.sql.Timestamp
        if (value instanceof Timestamp timestampValue) {
            long millis = timestampValue.getTime();
            int nanos = timestampValue.getNanos() % 1_000_000; // Extract nanoseconds not part of milliseconds
            long micros = millis * 1_000 + nanos / 1_000; // Convert to microseconds
            return Literal.ofTimestampNtz(micros);
        }

        // Handle java.time.LocalDateTime
        if (value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            long micros = toEpochMicros(localDateTime);
            return Literal.ofTimestampNtz(micros);
        }


        // BOOLEAN Handling (true -> epoch + 1 microsecond, false -> epoch)
        if (value instanceof Boolean) {
            boolean boolValue = (Boolean) value;
            long micros = boolValue ? 1 : 0;
            return Literal.ofTimestamp(micros);
        }

        throw new IllegalArgumentException("Unsupported value type for TIMESTAMP_NTZ conversion: " + value.getClass().getName());
    }

    /**
     * Convert a value to DATE.
     */
    public static Expression toDate(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null when converting to DATE.");
        }

        // Handle String (parse as "yyyy-MM-dd")
        if (value instanceof String stringValue) {
            try {
                int daysSinceEpoch = stringToDate(stringValue);
                return Literal.ofDate(daysSinceEpoch);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid STRING format for DATE: " + value, e);
            }
        }

        // Handle java.sql.Date
        if (value instanceof Date sqlDate) {
            long millis = sqlDate.getTime();
            int daysSinceEpoch = (int) (millis / (24L * 60 * 60 * 1000)); // Convert milliseconds to days
            return Literal.ofDate(daysSinceEpoch);
        }

        // Handle java.sql.Timestamp
        if (value instanceof Timestamp timestamp) {
            long millis = timestamp.getTime();
            int daysSinceEpoch = (int) (millis / (24L * 60 * 60 * 1000)); // Convert milliseconds to days
            return Literal.ofDate(daysSinceEpoch);
        }

        // Handle java.time.LocalDateTime
        if (value instanceof LocalDateTime localDateTime) {
            LocalDate localDate = localDateTime.toLocalDate(); // Extract LocalDate
            int daysSinceEpoch = (int) localDate.toEpochDay(); // Convert LocalDate to days since epoch
            return Literal.ofDate(daysSinceEpoch);
        }

        throw new IllegalArgumentException("Unsupported value type for DATE conversion: " + value.getClass().getName());
    }

    /**
     * Extract milliseconds from a numeric value.
     *
     * @param number the numeric value (byte, short, int, long, float, double, BigDecimal)
     * @return the equivalent milliseconds since epoch
     */
    private static long extractMillisFromNumber(Number number) {
        if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
            // Most common case first for better branch prediction
            // Treat as microseconds since epoch
            return number.longValue() * 1000; // Convert to milliseconds
        } else if (number instanceof Double || number instanceof Float) {
            // Treat as fractional microseconds since epoch
            return (long) (number.doubleValue() * 1000.0); // Convert to milliseconds
        } else if (number instanceof BigDecimal) {
            // BigDecimal for high-precision microseconds - no need for pattern matching variable
            // Avoid unnecessary division by 1
            return ((BigDecimal) number).longValue() * 1000;
        } else {
            throw new IllegalArgumentException("Unsupported numeric type: " + number.getClass().getName());
        }
    }

    /**
     * Parses a UTF8 timestamp string into a corresponding {@code Long} value, representing the
     * timestamp in microseconds. Returns {@code Optional.empty()} in case of invalid input.
     *
     * @param s          The input UTF8 timestamp string.
     * @param timeZoneId The default {@code ZoneId} to use if no time zone is specified in the input.
     * @return An {@code Optional<Long>} containing the timestamp value in microseconds, or empty if parsing fails.
     */
    public static long stringToTimestamp(String s, ZoneId timeZoneId) {
        try {
            Object[] parseResult = parseTimestampString(s);
            int[] segments = (int[]) parseResult[0];
            Optional<ZoneId> parsedZoneId = (Optional<ZoneId>) parseResult[1];
            boolean justTime = (boolean) parseResult[2];

            if (segments.length == 0) {
                return 0;
            }

            ZoneId zoneId = parsedZoneId.orElse(timeZoneId);

            // Convert nanoseconds
            long nanoseconds = segments[6] * 1000L;
            LocalTime localTime = LocalTime.of(segments[3], segments[4], segments[5], (int) nanoseconds);

            // If it's just time, use the current date
            LocalDate localDate = justTime ? LocalDate.now(zoneId)
                    : LocalDate.of(segments[0], segments[1], segments[2]);

            LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
            Instant instant = zonedDateTime.toInstant();

            return instantToMicros(instant);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Referring to <a href="https://github.com/apache/spark/blob/b557311d43947f1e862345ada89f4d27de74e7b1/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala#L478">...</a>
     * Helper to parse a timestamp string into segments (date, time, nanoseconds), ZoneId, and a flag
     * indicating if it's just time without a date.
     *
     * @param s The input UTF8 timestamp string.
     * @return An array containing:
     *         - Segments (int array with date/time components)
     *         - ZoneId (Optional<ZoneId>)
     *         - Just time flag (boolean)
     */
    private static Object[] parseTimestampString(String s) {
        if (s == null) {
            return new Object[]{new int[0], Optional.empty(), false};
        }

        Optional<String> tz = Optional.empty();
        int[] segments = new int[]{1, 1, 1, 0, 0, 0, 0, 0, 0};
        int i = 0;
        int currentSegmentValue = 0;
        int currentSegmentDigits = 0;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);

        // Trim leading spaces
        int j = 0;
        while (j < bytes.length && Character.isWhitespace(bytes[j])) {
            j++;
        }

        // Trim trailing spaces
        int strEndTrimmed = bytes.length;
        while (strEndTrimmed > j && Character.isWhitespace(bytes[strEndTrimmed - 1])) {
            strEndTrimmed--;
        }

        if (j == strEndTrimmed) {
            return new Object[]{new int[0], Optional.empty(), false};
        }

        int digitsMilli = 0;
        boolean justTime = false;
        Optional<Integer> yearSign = Optional.empty();

        if (bytes[j] == '-' || bytes[j] == '+') {
            yearSign = Optional.of(bytes[j] == '-' ? -1 : 1);
            j++;
        }

        while (j < strEndTrimmed) {
            byte b = bytes[j];
            int parsedValue = b - '0';

            if (parsedValue < 0 || parsedValue > 9) { // Non-digit characters
                if (j == 0 && b == 'T') {
                    justTime = true;
                    i += 3;
                } else if (i < 2 && b == '-') {
                    if (!isValidSegment(i, currentSegmentDigits)) {
                        return new Object[]{new int[0], Optional.empty(), false};
                    }
                    segments[i] = currentSegmentValue;
                    currentSegmentValue = 0;
                    currentSegmentDigits = 0;
                    i++;
                } else if (i == 2 && (b == ' ' || b == 'T')) {
                    if (!isValidSegment(i, currentSegmentDigits)) {
                        return new Object[]{new int[0], Optional.empty(), false};
                    }
                    segments[i] = currentSegmentValue;
                    currentSegmentValue = 0;
                    currentSegmentDigits = 0;
                    i++;
                } else if ((i == 3 || i == 4) && b == ':') {
                    if (!isValidSegment(i, currentSegmentDigits)) {
                        return new Object[]{new int[0], Optional.empty(), false};
                    }
                    segments[i] = currentSegmentValue;
                    currentSegmentValue = 0;
                    currentSegmentDigits = 0;
                    i++;
                } else if (i == 5 && b == '.') {
                    if (!isValidSegment(i, currentSegmentDigits)) {
                        return new Object[]{new int[0], Optional.empty(), false};
                    }
                    segments[i] = currentSegmentValue;
                    currentSegmentValue = 0;
                    currentSegmentDigits = 0;
                    i++;
                } else if (i == 6) {
                    if (!isValidSegment(i, currentSegmentDigits)) {
                        return new Object[]{new int[0], Optional.empty(), false};
                    }
                    segments[i] = currentSegmentValue;
                    currentSegmentValue = 0;
                    currentSegmentDigits = 0;
                    i++;
                    tz = Optional.of(new String(bytes, j, strEndTrimmed - j, StandardCharsets.UTF_8));
                    j = strEndTrimmed - 1;
                } else {
                    return new Object[]{new int[0], Optional.empty(), false};
                }
            } else { // Digit characters
                if (i == 6) {
                    digitsMilli++;
                }
                if (i != 6 || currentSegmentDigits < 6) {
                    currentSegmentValue = currentSegmentValue * 10 + parsedValue;
                }
                currentSegmentDigits++;
            }
            j++;
        }

        if (!isValidSegment(i, currentSegmentDigits)) {
            return new Object[]{new int[0], Optional.empty(), false};
        }
        segments[i] = currentSegmentValue;

        while (digitsMilli < 6) {
            segments[6] *= 10;
            digitsMilli++;
        }

        Optional<ZoneId> zoneId = tz.map(zoneName -> ZoneId.of(zoneName.trim()));
        segments[0] *= yearSign.orElse(1);
        return new Object[]{segments, zoneId, justTime};
    }

    private static boolean isValidSegment(int segment, int digits) {
        int maxDigitsYear = 6;
        return segment == 6 || (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
                (segment != 0 && segment != 6 && digits > 0 && digits <= 2);
    }

    private static long instantToMicros(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }


    /**
     * Referring to <a href="https://github.com/apache/spark/blob/b557311d43947f1e862345ada89f4d27de74e7b1/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala#L380">...</a>
     */
    public static Integer stringToDate(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Unsupported string value: " + s);
        }

        int[] segments = new int[]{1, 1, 1}; // Default to year=1, month=1, day=1
        int sign = 1; // Positive or negative year
        int i = 0; // Segment index
        int currentSegmentValue = 0;
        int currentSegmentDigits = 0;

        byte[] bytes = s.getBytes();
        int j = getTrimmedStart(bytes);
        int strEndTrimmed = getTrimmedEnd(j, bytes);

        if (j == strEndTrimmed) {
            throw new IllegalArgumentException("Unsupported string value: " + s);
        }

        // Handle optional '+' or '-' for year sign
        if (bytes[j] == '-' || bytes[j] == '+') {
            sign = (bytes[j] == '-') ? -1 : 1;
            j++;
        }

        while (j < strEndTrimmed && (i < 3 && !(bytes[j] == ' ' || bytes[j] == 'T'))) {
            byte b = bytes[j];
            if (i < 2 && b == '-') { // Segment separator for year, month, day
                if (!isValidDigits(i, currentSegmentDigits)) {
                    throw new IllegalArgumentException("Unsupported string value: " + s);
                }
                segments[i] = currentSegmentValue;
                currentSegmentValue = 0;
                currentSegmentDigits = 0;
                i++;
            } else {
                int parsedValue = b - '0';
                if (parsedValue < 0 || parsedValue > 9) { // Non-digit character
                    throw new IllegalArgumentException("Unsupported string value: " + s);
                }
                currentSegmentValue = currentSegmentValue * 10 + parsedValue;
                currentSegmentDigits++;
            }
            j++;
        }

        // Validate the final segment
        if (!isValidDigits(i, currentSegmentDigits)) {
            throw new IllegalArgumentException("Unsupported string value: " + s);
        }
        if (i < 2 && j < strEndTrimmed) {
            // For `yyyy` and `yyyy-[m]m` formats, the entire input must be consumed
            throw new IllegalArgumentException("Unsupported string value: " + s);
        }
        segments[i] = currentSegmentValue;

        try {
            // Construct LocalDate and convert it to days since epoch
            LocalDate localDate = LocalDate.of(sign * segments[0], segments[1], segments[2]);
            return localDateToDays(localDate);
        } catch (Exception e) {
            // Catch invalid date exceptions
            throw new IllegalArgumentException("Unsupported string value: " + s);
        }
    }

    // Helper method to validate digit counts for a segment
    private static boolean isValidDigits(int segment, int digits) {
        int maxDigitsYear = 7; // A year can be up to 7 digits long
        return (segment == 0 && digits >= 4 && digits <= maxDigitsYear) || // Year
                (segment != 0 && digits > 0 && digits <= 2);               // Month/Day
    }

    // Helper method to trim leading whitespace
    private static int getTrimmedStart(byte[] bytes) {
        int i = 0;
        while (i < bytes.length && Character.isWhitespace(bytes[i])) {
            i++;
        }
        return i;
    }

    // Helper method to trim trailing whitespace
    private static int getTrimmedEnd(int start, byte[] bytes) {
        int i = bytes.length;
        while (i > start && Character.isWhitespace(bytes[i - 1])) {
            i--;
        }
        return i;
    }

    // Helper method to convert LocalDate to days since epoch
    private static int localDateToDays(LocalDate localDate) {
        return (int) localDate.toEpochDay();
    }

    /**
     * Convert a LocalDateTime to microseconds since epoch.
     *
     * @param localDateTime the LocalDateTime to convert
     * @return microseconds since epoch
     */
    private static long toEpochMicros(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1_000_000L
                + localDateTime.getNano() / 1_000L; // Convert nanoseconds to microseconds
    }
}