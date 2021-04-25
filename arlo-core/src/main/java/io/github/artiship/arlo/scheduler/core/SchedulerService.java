package io.github.artiship.arlo.scheduler.core;

import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

public interface SchedulerService extends Service {
    SchedulerTaskBo submit(SchedulerTaskBo task);

    boolean kill(SchedulerTaskBo task);
}
