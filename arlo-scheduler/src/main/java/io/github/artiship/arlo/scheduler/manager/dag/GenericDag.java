package io.github.artiship.arlo.scheduler.manager.dag;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

public interface GenericDag {
    void start();

    void advance(SchedulerTaskBo task);

    boolean isCompleted();

    void stop();

    Long getId();

    String getName();
}
