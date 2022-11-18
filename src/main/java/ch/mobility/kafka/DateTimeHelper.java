package ch.mobility.kafka;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class DateTimeHelper {

    private DateTimeHelper() {}

    public static String format(TemporalAccessor temporal) {
        if (temporal != null) {
            return DateTimeFormatter.ISO_INSTANT.format(temporal);
        }
        return null;
    }
}
