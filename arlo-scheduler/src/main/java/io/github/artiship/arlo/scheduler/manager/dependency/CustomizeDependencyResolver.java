package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.model.bo.TaskDependency;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.Builder;

import java.util.List;

@Builder
public class CustomizeDependencyResolver implements DependencyResolver {
    private SchedulerTaskBo task;
    private SchedulerJobBo parentJob;

    public CustomizeDependencyResolver(SchedulerTaskBo task, SchedulerJobBo parentJob) {
        this.task = task;
        this.parentJob = parentJob;
    }

    @Override
    public List<TaskDependency> dependencies() {
        return null;
    }
}
