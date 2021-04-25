package io.github.artiship.arlo.model.enums;

import java.util.List;

import static java.util.Arrays.asList;

public enum JobType {
    MYSQL2HIVE(1),
    HIVE2MYSQL(2),
    HQL(3),
    SHELL(4),
    PYTHON(5),
    SPARK(6),
    FILE_CHECK(7),
    TABLEAU(8),
    CALLABLE(9),
    DQC(10),
    SPARK_SUBMIT(11),
    YARN_MONITOR(12),
    UNKNOWN(-1);

    private int code;

    JobType(int code) {
        this.code = code;
    }

    public static JobType of(int code) {
        for (JobType jobType : JobType.values()) {
            if (jobType.code == code) {
                return jobType;
            }
        }
        return UNKNOWN;
    }

    public static JobType getJobTypeByName(String name) {
        for (JobType jobType : JobType.values()) {
            if (jobType.toString()
                       .equalsIgnoreCase(name)) {
                return jobType;
            }
        }
        throw new IllegalArgumentException("unsupported job name " + name);
    }

    public static List<Integer> esQueryType() {
        return asList(HQL.getCode(), SHELL.getCode(), PYTHON.getCode());
    }

    public static List<Integer> hasResourceType() {
        return asList(MYSQL2HIVE.getCode(), HIVE2MYSQL.getCode(), SPARK_SUBMIT.getCode(), HQL.getCode());
    }


    public int getCode() {
        return this.code;
    }
}