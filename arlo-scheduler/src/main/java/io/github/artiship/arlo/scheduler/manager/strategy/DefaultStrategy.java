package io.github.artiship.arlo.scheduler.manager.strategy;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.Optional;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;


public class DefaultStrategy implements Strategy {

    public Optional<SchedulerNodeBo> select(List<SchedulerNodeBo> list,
                                            SchedulerTaskBo task,
                                            List<String> lastFailedHosts) {
        return getWorker(list,
                task,
                workers -> workers.stream()
                                  .filter(w -> memUsageUnder90percent(w.getMemoryUsage()))
                                  .sorted(comparing(SchedulerNodeBo::availableSlots, reverseOrder()))
                                  .findFirst()
                                  .orElse(null),
                lastFailedHosts);
    }

    private boolean memUsageUnder90percent(Double memUsage) {
        if (memUsage == null) return true;
        return memUsage < 90.0;
    }
}