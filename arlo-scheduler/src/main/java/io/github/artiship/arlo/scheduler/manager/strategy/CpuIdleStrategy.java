package io.github.artiship.arlo.scheduler.manager.strategy;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.Optional;

import static java.util.Comparator.comparing;

public class CpuIdleStrategy implements Strategy {

    @Override
    public Optional<SchedulerNodeBo> select(List<SchedulerNodeBo> list, SchedulerTaskBo task, List<String> lastFailedHosts) {
        return getWorker(list,
                task,
                workers -> workers.stream()
                                  .sorted(comparing(SchedulerNodeBo::getCpuUsage))
                                  .findFirst()
                                  .get(),
                lastFailedHosts);
    }
}