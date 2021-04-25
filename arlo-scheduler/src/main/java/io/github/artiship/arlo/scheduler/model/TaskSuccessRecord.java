package io.github.artiship.arlo.scheduler.model;

import io.github.artiship.arlo.model.bo.TaskDependency;
import lombok.Data;

import java.time.LocalDateTime;

import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.Dates.localDateTimeToStr;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

@Data
public class TaskSuccessRecord implements Comparable<TaskSuccessRecord> {
    private final Long taskId;
    private final LocalDateTime scheduleTime;
    private final String calTimeRange;
    private final String taskCron;
    private final Long executionCost;

    private TaskSuccessRecord(Long taskId,
                              String taskCron,
                              LocalDateTime scheduleTime,
                              Long executionCost) {
        this.taskId = taskId;
        this.taskCron = taskCron;
        this.scheduleTime = scheduleTime;
        this.executionCost = executionCost;
        this.calTimeRange = calTimeRangeStr(scheduleTime, taskCron);
    }

    public static TaskSuccessRecord of(Long taskId,
                                       String taskCron,
                                       LocalDateTime scheduleTime,
                                       Long executionCost) {
        requireNonNull(taskCron, "Cron is null");
        requireNonNull(scheduleTime, "Schedule time is null");

        return new TaskSuccessRecord(taskId, taskCron, scheduleTime, executionCost);
    }

    @Override
    public int compareTo(TaskSuccessRecord lastRecord) {
        return scheduleTime.truncatedTo(SECONDS)
                           .compareTo(lastRecord.getScheduleTime()
                                                .truncatedTo(SECONDS));
    }

    public boolean scheduleTimeEquals(LocalDateTime scheduleTime) {
        return this.scheduleTime.truncatedTo(SECONDS)
                                .isEqual((scheduleTime.truncatedTo(SECONDS)));
    }

    public boolean cronEquals(String cron) {
        return this.taskCron.equals(cron);
    }

    public String scheduleTimeStr() {
        return localDateTimeToStr(this.scheduleTime);
    }

    public String getCalTimeRangeStr() {
        return this.calTimeRange;
    }

    public TaskDependency toTaskDependency() {
        return new TaskDependency().setTaskId(this.taskId)
                                   .setCalTimeRange(this.calTimeRange)
                                   .setScheduleTime(this.scheduleTimeStr())
                                   .setTaskCron(this.taskCron);
    }
}
