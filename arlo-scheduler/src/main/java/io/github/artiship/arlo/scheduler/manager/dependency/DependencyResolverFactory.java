package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

public class DependencyResolverFactory {
    public static DependencyResolver create(final SchedulerTaskBo task, final SchedulerJobBo parentJob) {
        DependencyResolver dependencyResolver;
        switch (task.getDependencyType()) {
            case DEFAULT:
                dependencyResolver = DefaultDependencyResolver.builder()
                                                              .childCronExpression(task.getScheduleCron())
                                                              .childScheduleTime(task.getScheduleTime())
                                                              .parentCronExpression(parentJob.getScheduleCron())
                                                              .isParentSelfDepend(parentJob.getIsSelfDependent())
                                                              .build();
                break;
            case CUSTOMIZE:
                dependencyResolver = CustomizeDependencyResolver.builder()
                                                                .childScheduleTime(task.getScheduleTime())
                                                                .dependencyRange(task.getDependencyRange())
                                                                .dependencyRule(task.getDependencyRule())
                                                                .parentCronExpression(parentJob.getScheduleCron())
                                                                .isParentSelfDepend(parentJob.isSelfDependent())
                                                                .build();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + task.getDependencyType());
        }
        return dependencyResolver;
    }
}
