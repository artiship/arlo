package io.github.artiship.arlo.scheduler.manager.strategy;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public interface Strategy {
    Optional<SchedulerNodeBo> select(List<SchedulerNodeBo> workers, SchedulerTaskBo task, List<String> lastFailedHosts);

    default Optional<SchedulerNodeBo> getWorker(List<SchedulerNodeBo> workers,
                                                SchedulerTaskBo task,
                                                Function<List<SchedulerNodeBo>, SchedulerNodeBo> strategy,
                                                List<String> lastFailedHosts) {

        List<SchedulerNodeBo> selectedWorkers = workers.stream()
                                                       .filter(w -> w.notInFailedList(lastFailedHosts))
                                                       .collect(toList());

        if (selectedWorkers.isEmpty()) {
            selectedWorkers = workers;
        }

        List<SchedulerNodeBo> filtered = selectedWorkers.stream()
                                                        .filter(w -> w.isActive())
                                                        .filter(w -> w.hasTaskSlots())
                                                        .filter(w -> w.inWorkerGroups(task.getWorkerGroups()))
                                                        .collect(toList());

        return ofNullable(strategy.apply(filtered));
    }
}