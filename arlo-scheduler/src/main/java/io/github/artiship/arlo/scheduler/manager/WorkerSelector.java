package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.strategy.CpuIdleStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.DefaultStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.MemoryFreeStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.RandomStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.RoundRobinStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.Strategy;
import io.github.artiship.arlo.scheduler.manager.strategy.TaskIdleStrategy;
import io.github.artiship.arlo.scheduler.manager.strategy.WorkerSelectStrategyEnum;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.util.Lists.emptyList;

public class WorkerSelector {

    private static Map<WorkerSelectStrategyEnum, Strategy> strategies = new ConcurrentHashMap<>();

    static {
        strategies.put(WorkerSelectStrategyEnum.DEFAULT, new DefaultStrategy());
        strategies.put(WorkerSelectStrategyEnum.MEMORY, new MemoryFreeStrategy());
        strategies.put(WorkerSelectStrategyEnum.CPU, new CpuIdleStrategy());
        strategies.put(WorkerSelectStrategyEnum.TASK, new TaskIdleStrategy());
        strategies.put(WorkerSelectStrategyEnum.RANDOM, new RandomStrategy());
        strategies.put(WorkerSelectStrategyEnum.ROBIN, new RoundRobinStrategy());
    }

    private Strategy strategy;
    private List<SchedulerNodeBo> workers;
    private SchedulerTaskBo task;
    private List<String> lastFailedHosts;

    private WorkerSelector() {

    }

    public static WorkerSelector builder() {
        return new WorkerSelector();
    }

    public WorkerSelector withStrategy(Strategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public WorkerSelector withWorkers(List<SchedulerNodeBo> workers) {
        this.workers = workers;
        return this;
    }

    public WorkerSelector withLastFailedHosts(List<String> lastFailedHosts) {
        this.lastFailedHosts = lastFailedHosts;
        return this;
    }

    public WorkerSelector withTask(SchedulerTaskBo task) {
        this.task = task;
        return this;
    }

    public Optional<SchedulerNodeBo> select() {
        requireNonNull(task, "Task is null");
        requireNonNull(workers, "Workers is null");

        if (lastFailedHosts == null) {
            lastFailedHosts = emptyList();
        }

        if (strategy == null) {
            strategy = strategies.get(WorkerSelectStrategyEnum.DEFAULT);
        }

        return strategy.select(workers, task, lastFailedHosts);
    }
}
