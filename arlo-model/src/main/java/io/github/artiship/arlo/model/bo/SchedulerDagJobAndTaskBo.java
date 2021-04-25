package io.github.artiship.arlo.model.bo;

import io.github.artiship.arlo.model.entity.SchedulerJob;
import io.github.artiship.arlo.model.entity.SchedulerTask;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.LinkedList;
import java.util.List;

@Accessors(chain = true)
@Data
public class SchedulerDagJobAndTaskBo {

    private Long taskId;
    private Long jobId;
    private String jobName;
    private String cron;
    private String ownerNames;
    private String scheduleTime;
    private String startTime;
    private String endTime;
    private Integer state;
    private String range;
    private Integer jobType;

    public static List<SchedulerDagJobAndTaskBo> from(Iterable<SchedulerJob> parentTasks) {
        List<SchedulerDagJobAndTaskBo> list = new LinkedList();
        parentTasks.forEach(schedulerJob -> list.add(from(schedulerJob)));
        return list;
    }

    public static SchedulerDagJobAndTaskBo from(SchedulerJob schedulerJob) {
        return from(schedulerJob, null);
    }

    public static SchedulerDagJobAndTaskBo from(SchedulerJob schedulerJob, String calTimeRange) {
        return new SchedulerDagJobAndTaskBo()
                .setJobId(schedulerJob.getId())
                .setCron(schedulerJob.getScheduleCron())
                .setJobName(schedulerJob.getJobName())
                .setOwnerNames(schedulerJob.getOwnerNames())
                .setState(schedulerJob.getJobReleaseState())
                .setJobType(schedulerJob.getJobType())
                .setRange(calTimeRange);
    }

    public static SchedulerDagJobAndTaskBo of(Long jobId, String cron, String ownerNames, String jobName, String scheduleTime) {
        return new SchedulerDagJobAndTaskBo()
                .setJobId(jobId)
                .setCron(cron)
                .setOwnerNames(ownerNames)
                .setJobName(jobName)
                .setScheduleTime(scheduleTime);
    }

    public static SchedulerDagJobAndTaskBo from(SchedulerTask task, TaskDependency taskDependency) {
        return new SchedulerDagJobAndTaskBo()
                .setJobId(task.getJobId())
                .setCron(task.getScheduleCron())
                .setJobName(task.getTaskName())
                .setOwnerNames(task.getCreatorName())
                .setTaskId(task.getId())
                .setJobType(task.getJobType())
                .setScheduleTime(taskDependency.getScheduleTime())
                .setRange(taskDependency.getCalTimeRange())
                .setStartTime(task.getStartTimeStr())
                .setEndTime(task.getEndTimeStr())
                .setState(task.getTaskState());

    }
}
