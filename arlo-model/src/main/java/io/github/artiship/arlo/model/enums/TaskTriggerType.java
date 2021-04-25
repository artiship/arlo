package io.github.artiship.arlo.model.enums;

import java.util.List;

import static java.util.Arrays.asList;

public enum TaskTriggerType {
    CRON(1, "周期调度"),
    MANUAL_RUN(2, "立即执行"),
    MANUAL_FREE(21, "手动解除依赖"),
    MANUAL_RERUN(22, "手动重跑"),
    MANUAL_COMPLEMENT(23, "手动补数据"),
    MANUAL_COMPLEMENT_DOWNSTREAM(24, "手动重跑下游"),
    FAILOVER(3, "失败转移"),
    AUTO_RETRY(4, "自动重试");

    private int code;
    private String desc;

    TaskTriggerType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static TaskTriggerType of(int code) {
        for (TaskTriggerType triggerType : TaskTriggerType.values()) {
            if (triggerType.code == code) {
                return triggerType;
            }
        }
        throw new IllegalArgumentException("unsupported action type " + code);
    }

    public static List<TaskTriggerType> manualTypes() {
        return asList(MANUAL_RUN, MANUAL_FREE, MANUAL_RERUN, MANUAL_COMPLEMENT, MANUAL_COMPLEMENT_DOWNSTREAM);
    }

    public static List<TaskTriggerType> manualTypesExceptSingleCompletement() {
        return asList(MANUAL_RUN, MANUAL_FREE, MANUAL_RERUN, MANUAL_COMPLEMENT, MANUAL_COMPLEMENT_DOWNSTREAM);
    }

    public int getCode() {
        return this.code;
    }
}
