package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.enums.NodeState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.strategy.DefaultStrategy;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

public class WorkerSelectStrategyTest {

    @Test
    public void defaultStrategyTest() {
        SchedulerNodeBo worker1 = new SchedulerNodeBo().setNodeState(NodeState.ACTIVE)
                                                       .setId(1L)
                                                       .setMemoryUsage(null)
                                                       .setRunningTasks(null)
                                                       .setMaxTasks(null);

        SchedulerNodeBo worker2 = new SchedulerNodeBo().setNodeState(NodeState.ACTIVE)
                                                       .setId(2L)
                                                       .setMemoryUsage(80.0)
                                                       .setRunningTasks(4)
                                                       .setMaxTasks(10);

        SchedulerNodeBo worker3 = new SchedulerNodeBo().setNodeState(NodeState.ACTIVE)
                                                       .setId(3L)
                                                       .setMemoryUsage(80.0)
                                                       .setRunningTasks(3)
                                                       .setMaxTasks(10);

        Optional<SchedulerNodeBo> worker = WorkerSelector.builder()
                                                         .withWorkers(Arrays.asList(worker1, worker2, worker3))
                                                         .withTask(new SchedulerTaskBo().setTaskName("test"))
                                                         .withStrategy(new DefaultStrategy())
                                                         .select();

        System.out.println(worker);
    }
}
