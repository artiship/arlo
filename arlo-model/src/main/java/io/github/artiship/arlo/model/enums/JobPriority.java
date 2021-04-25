package io.github.artiship.arlo.model.enums;

public enum JobPriority {

    LOW(1), MEDIUM(2), HIGH(3), HIGHEST(4);

    private Integer priority;

    JobPriority(int priority) {
        this.priority = priority;
    }

    public static JobPriority of(int priority) {
        for (JobPriority jobPriority : JobPriority.values()) {
            if (jobPriority.priority == priority) {
                return jobPriority;
            }
        }
        throw new IllegalArgumentException("unsupported node status " + priority);
    }

    public Integer getPriority() {
        return this.priority;
    }
}
