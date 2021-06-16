package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.model.bo.TaskDependency;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.stream.Collectors;

import static io.github.artiship.arlo.scheduler.manager.dependency.DefaultDependencyBuilder.builder;
import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.Dates.localDateTimeToStr;

public class DefaultDependencyResolver implements DependencyResolver {
    private SchedulerJobBo parentJob;
    private SchedulerTaskBo task;

    public DefaultDependencyResolver(SchedulerTaskBo task, SchedulerJobBo parentJob) {
        this.task = task;
        this.parentJob = parentJob;
    }

    @Override
    public List<TaskDependency> dependencies() {
        return builder().childCronExpression(task.getScheduleCron())
                        .childScheduleTime(task.getScheduleTime())
                        .parentCronExpression(parentJob.getScheduleCron())
                        .isParentSelfDepend(parentJob.getIsSelfDependent())
                        .build()
                        .parentScheduleTimes()
                        .stream()
                        .map(scheduleTime ->
                                new TaskDependency(parentJob.getId(),
                                        localDateTimeToStr(scheduleTime), calTimeRangeStr(scheduleTime, parentJob.getScheduleCron()))
                        )
                        .collect(Collectors.toList());


    }
}
