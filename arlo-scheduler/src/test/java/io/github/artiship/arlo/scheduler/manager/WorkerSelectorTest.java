package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.strategy.MemoryFreeStrategy;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.github.artiship.arlo.model.enums.NodeState.ACTIVE;
import static org.assertj.core.api.Assertions.assertThat;

public class WorkerSelectorTest {

    @Test
    public void test_select() {
        SchedulerNodeBo node = new SchedulerNodeBo().setNodeState(ACTIVE)
                                                    .setWorkerGroups(Arrays.asList("spark"));

        assertThat(node.hasTaskSlots()).isFalse();

        node.setMaxTasks(10)
            .setRunningTasks(5);

        assertThat(node.hasTaskSlots()).isTrue();
        Optional<SchedulerNodeBo> worker = WorkerSelector.builder()
                                                         .withWorkers(Arrays.asList(node))
                                                         .withStrategy(new MemoryFreeStrategy())
                                                         .withTask(new SchedulerTaskBo().setId(1L))
                                                         .select();

        assertThat(worker.isPresent()).isTrue();
    }

}