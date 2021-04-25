package io.github.artiship.arlo.scheduler.manager.strategy;

public enum WorkerSelectStrategyEnum {
    DEFAULT(0),
    TASK(1),
    MEMORY(2),
    CPU(3),
    RANDOM(4),
    ROBIN(5);

    private int code;

    WorkerSelectStrategyEnum(int code) {
        this.code = code;
    }

    public static WorkerSelectStrategyEnum of(int code) {
        for (WorkerSelectStrategyEnum js : WorkerSelectStrategyEnum.values()) {
            if (js.code == code) {
                return js;
            }
        }
        return WorkerSelectStrategyEnum.TASK;
    }

    public static WorkerSelectStrategyEnum from(String name) {
        for (WorkerSelectStrategyEnum js : WorkerSelectStrategyEnum.values()) {
            if (js.toString()
                  .equalsIgnoreCase(name)) {
                return js;
            }
        }
        return WorkerSelectStrategyEnum.TASK;
    }
}