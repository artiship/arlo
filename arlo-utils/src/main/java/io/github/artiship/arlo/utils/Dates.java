package io.github.artiship.arlo.utils;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.parse;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Date.from;

public class Dates {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final long MILLIS_PER_SECOND = 1000;

    public static LocalDateTime localDateTime(Date date) {
        return date.toInstant()
                   .atZone(systemDefault())
                   .toLocalDateTime();
    }

    public static LocalDateTime localDateTime(Timestamp ts) {
        return localDateTime(date(ts));
    }

    public static LocalDateTime localDateTime(String dateTimeStr) {
        return parse(dateTimeStr, ofPattern(DATE_TIME_FORMAT));
    }

    public static String localDateTimeToStr(LocalDateTime localDateTime) {
        return localDateTime.format(ofPattern(DATE_TIME_FORMAT));
    }

    public static String localDateTimeToDate(LocalDateTime localDateTime) {
        return localDateTime.format(ofPattern(DATE_FORMAT));
    }

    public static Date date(LocalDateTime localDateTime) {
        return from(instant(localDateTime));
    }

    public static Date date(Timestamp timestamp) {
        return new Date(timestamp.getSeconds() * MILLIS_PER_SECOND);
    }

    public static String dateToStr(Date date) {
        return localDateTimeToStr(localDateTime(date));
    }

    public static Timestamp protoTimestamp(LocalDateTime localDateTime) {
        if (localDateTime == null)
            return null;

        return protoTimestamp(from(instant(localDateTime)));
    }

    public static Timestamp protoTimestamp(Date date) {
        return fromMillis(date.getTime());
    }

    public static String getLocalDateTimeAtTodayBeginingToStr() {
        return localDateTimeToStr(LocalDateTime.of(now().getYear(), now().getMonth(), now().getDayOfMonth(), 0, 0, 0));
    }

    private static Instant instant(LocalDateTime localDateTime) {
        return localDateTime.atZone(systemDefault())
                            .toInstant();
    }

    public static String getNow() {
        return LocalDateTime.now()
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
