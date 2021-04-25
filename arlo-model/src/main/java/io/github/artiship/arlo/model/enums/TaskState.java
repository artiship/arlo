package io.github.artiship.arlo.model.enums;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

/**
 * |----------->|------------------->|----------killing------->|-------------------------------|
 * |            ^                    ^                         |                               |
 * |            |<------failover-----|                       killed                         kill_fail
 * |            |                    |                         |                               |
 * |            |                    |                         |                               |
 * (start)--->pending---->waiting---dispatched-->running------success--->(end)<-----------------------------|
 * ^                    |                         ^                               |
 * |                    |                         |                               |
 * |                    |---------------------->failed                            |
 * |                                              |                               |
 * |-------------------------retry----------------|-------exceed retry times------|
 */
public enum TaskState {
    PENDING(1, "Pending for parents to accomplish", "等待依赖"),
    WAITING(2, "Waiting for available worker to execute", "待分发"),
    WAITING_PARALLELISM_LIMIT(21, "Task is limited by the job max concurrency", "并发受限"),
    WAITING_TASK_SLOT(22, "Waiting for available worker or task slot", "资源不足"),
    WAITING_COMPLEMENT_LIMIT(23, "10 ~ 24", "补数限制"),
    WAITING_EXTRACT_LIMIT(24, "serial", "抽取串行"),
    DISPATCHED(3, "Dispatched to worker", "已分发"),
    RUNNING(4, "Executing in a worker node", "运行中"),
    TO_KILL(5, "To kill ", "待终止"),
    KILLED(6, "Killed", "已终止"),
    KILL_FAIL(7, "Kill failed", "终止失败"),
    FAILOVER(8, "When worker down", "失败转移"),
    SUCCESS(9, "Success", "成功"),
    FAIL(10, "Failed", "失败"),
    IGNORE(11, "ignore", "跳过"),
    UNKNOWN(-1, "", "未知");

    private Integer code;
    private String desc;
    private String descCn;

    TaskState(Integer code, String desc, String descCn) {
        this.code = code;
        this.desc = desc;
        this.descCn = descCn;
    }

    public static TaskState of(int code) {
        for (TaskState taskState : TaskState.values()) {
            if (taskState.code == code) {
                return taskState;
            }
        }
        return UNKNOWN;
    }

    public static List<TaskState> finishStates() {
        return asList(KILLED, FAILOVER, SUCCESS, FAIL);
    }

    public static List<Integer> finishStateCodes() {
        return finishStates().stream()
                             .map(s -> s.getCode())
                             .collect(toList());
    }

    public static List<TaskState> waitingStates() {
        return asList(WAITING, WAITING_TASK_SLOT, WAITING_PARALLELISM_LIMIT, WAITING_EXTRACT_LIMIT, WAITING_COMPLEMENT_LIMIT);
    }

    public static List<Integer> waitingStateCodes() {
        return waitingStates().stream()
                              .map(s -> s.getCode())
                              .collect(toList());
    }

    public static List<TaskState> unFinishedStates() {
        return stream(TaskState.values()).filter(s -> !finishStates().contains(s))
                                         .collect(toList());
    }

    public static List<Integer> unFinishedStateCodes() {
        return unFinishedStates().stream()
                                 .map(s -> s.getCode())
                                 .collect(toList());
    }

    public int getCode() {
        return this.code;
    }

    public String getDesc() {
        return this.descCn;
    }
}
