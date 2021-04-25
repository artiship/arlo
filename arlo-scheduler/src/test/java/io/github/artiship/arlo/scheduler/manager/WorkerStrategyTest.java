package io.github.artiship.arlo.scheduler.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.artiship.arlo.model.bo.DagNode;
import io.github.artiship.arlo.model.enums.NodeState;
import io.github.artiship.arlo.model.vo.ComplementDownStreamRequest;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.strategy.DefaultStrategy;
import io.github.artiship.arlo.utils.Dates;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class WorkerStrategyTest {

    @Test
    public void default_strategy_test() {
        DefaultStrategy defaultStrategy = new DefaultStrategy();
        SchedulerNodeBo worker_1 = new SchedulerNodeBo().setHost("1")
                                                        .setRunningTasks(30)
                                                        .setNodeState(NodeState.ACTIVE)
                                                        .setMaxTasks(30);
        SchedulerNodeBo worker_2 = new SchedulerNodeBo().setHost("2")
                                                        .setNodeState(NodeState.ACTIVE)
                                                        .setRunningTasks(0)
                                                        .setMaxTasks(30);
        Optional<SchedulerNodeBo> select = defaultStrategy.select(Arrays.asList(worker_1, worker_2), new SchedulerTaskBo(), Lists
                .emptyList());

        assertThat(select.get()
                         .getHost()).isEqualTo("2");
    }

    @Test
    public void test() throws IOException {
        ComplementDownStreamRequest request = new ComplementDownStreamRequest();
        Set<DagNode> set = new HashSet<>();
        DagNode dagNode = new DagNode(1L);
        dagNode.setChildren(new HashSet<>());
        request.setDag(set)
               .setStartTime(Dates.localDateTimeToStr(LocalDateTime.now()))
               .setEndTime(Dates.localDateTimeToStr(LocalDateTime.now()))
               .setJobId(2L)
               .setParallelism(2);

        ObjectMapper objectMapper = new ObjectMapper();
        String valueAsString = objectMapper.writeValueAsString(request);

        ComplementDownStreamRequest req = objectMapper.readValue(valueAsString, ComplementDownStreamRequest.class);
        log.info("log info {}", req);
    }
}
