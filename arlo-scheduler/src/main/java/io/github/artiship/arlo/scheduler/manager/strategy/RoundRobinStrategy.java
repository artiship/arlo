package io.github.artiship.arlo.scheduler.manager.strategy;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.Optional;


public class RoundRobinStrategy implements Strategy {

    private static Integer pos = 0;

    @Override
    public Optional<SchedulerNodeBo> select(List<SchedulerNodeBo> list,
                                            SchedulerTaskBo task,
                                            List<String> lastFailedHosts) {
        return getWorker(list,
                task,
                workers -> {
                    synchronized (pos) {
                        if (pos >= workers.size()) {
                            pos = 0;
                        }
                        return workers.get(pos++);
                    }
                },
                lastFailedHosts);
    }
}