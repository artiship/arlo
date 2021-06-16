package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.model.bo.TaskDependency;

import java.util.List;

public interface DependencyResolver {
    List<TaskDependency> dependencies();
}
