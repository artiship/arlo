package io.github.artiship.arlo.model.enums;

import java.time.temporal.ChronoUnit;

import static java.lang.Math.round;
import static java.time.temporal.ChronoUnit.*;

public enum JobCycle {

    SECOND(0, "秒"), MINUTE(1, "分钟"), HOUR(2, "小时"), DAY(3, "日"), WEEK(4, "周"), MONTH(5, "月"), YEAR(6, "年"), NONE(7);

    private static final long minute = 60_000;
    private static final long hour = 60 * minute;
    private static final long day = 24 * hour;
    private static final long week = 7 * day;
    private static final long month = 28 * day;
    private static final long year = 365 * day;
    private static final int factor = 2;
    private int code;
    private String desc;

    JobCycle(int code) {
        this.code = code;
    }

    JobCycle(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static JobCycle of(int code) {
        for (JobCycle jobCycle : JobCycle.values()) {
            if (jobCycle.code == code) {
                return jobCycle;
            }
        }
        throw new IllegalArgumentException("unsupported job cycle " + code);
    }

    public static JobCycle from(Long interval) {
        if (interval < minute) return SECOND;
        if (interval >= minute && interval < hour) return MINUTE;
        if (interval >= hour && interval < day) return HOUR;
        if (interval >= day && interval < week) return DAY;
        if (interval >= week && interval < month) return WEEK;
        if (interval >= month) return MONTH;

        return NONE;
    }

    public static Integer numberOfCycles(Long interval) {
        return round(interval / from(interval).cycleInterval());
    }

    public static ChronoUnit truncateUnit(Long interval) {
        switch (from(interval)) {
            case MINUTE:
                return MINUTES;
            case HOUR:
                return HOURS;
            case DAY:
                return DAYS;
            case WEEK:
                return WEEKS;
            case MONTH:
                return MONTHS;
            case YEAR:
                return DAYS;
            case SECOND:
                return SECONDS;
        }

        throw new RuntimeException("Interval " + interval + " has no matched truncate unit");
    }

    public Long cycleInterval() {
        switch (this) {
            case MINUTE:
                return minute;
            case HOUR:
                return hour;
            case DAY:
                return day;
            case WEEK:
                return week;
            case MONTH:
                return month;
            case YEAR:
                return year;
        }

        return null;
    }

    public Integer historySize() {
        int size;
        switch (this) {
            case DAY:
                size = 365;  // 1 year
                break;
            case HOUR:
                size = 169; // 1 week + 1   week -> hour
                break;
            case WEEK:
                size = 6;   // 1 month      month -> week
                break;
            case MONTH:
                size = 2;   // 2 months     month -> month
                break;
            case MINUTE:
                size = 1141;// 1 day + 1    day   -> minute
                break;
            default:
                size = 100;
                break;
        }
        return size * factor;
    }

    public int getCode() {
        return this.code;
    }

    public String getDesc() {
        return desc;
    }
}