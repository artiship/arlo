package io.github.artiship.arlo.scheduler.model;

import lombok.Data;

import java.time.LocalDateTime;

import static java.time.LocalDateTime.now;
import static java.util.Objects.requireNonNull;

@Data
public class TaskFailedRecord implements Comparable<TaskFailedRecord> {
    private LocalDateTime createTime;
    private String workerHost;

    private TaskFailedRecord(LocalDateTime createTime, String workerHost) {
        this.createTime = createTime;
        this.workerHost = workerHost;
    }

    public static TaskFailedRecord of(LocalDateTime createTime, String workerHost) {
        return new TaskFailedRecord(
                requireNonNull(createTime, "Create time is null"),
                requireNonNull(workerHost, "Worker host is null")
        );
    }

    @Override
    public int compareTo(TaskFailedRecord o) {
        return this.workerHost.compareTo(o.getWorkerHost());
    }

    public boolean latestOneDay() {
        return createTime.isAfter(now().plusDays(1));
    }
}
