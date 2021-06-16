package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

public class DependencyResolverFactory {
    public static DependencyResolver create(final SchedulerTaskBo task, final SchedulerJobBo parentJob) {
        DependencyResolver dependencyResolver;
        switch (task.getDependencyType()) {
            case DEFAULT:
                dependencyResolver = new DefaultDependencyResolver(task, parentJob);
                break;
            case CUSTOMIZE:
                dependencyResolver = new CustomizeDependencyResolver(task, parentJob);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + task.getDependencyType());
        }
        return dependencyResolver;
    }
}
