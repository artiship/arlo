package io.github.artiship.arlo.scheduler.manager;

import com.google.protobuf.Timestamp;
import io.github.artiship.arlo.model.enums.JobType;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.utils.Dates;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZoneId.systemDefault;

@Slf4j
public class ProtoTimestampTest {
    @Test
    public void test() {
        ZoneId zoneId = systemDefault();
        Timestamp timestamp = fromMillis(currentTimeMillis());
        LocalDateTime localDateTime = ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos())
                .atZone(zoneId)
                .toLocalDateTime();

        log.info("{}, {}, {}", zoneId, timestamp, localDateTime);
    }

    @Test
    public void test_converter() {
        Timestamp timestamp = Dates.protoTimestamp(LocalDateTime.now());
        LocalDateTime localDateTime = Dates.localDateTime(timestamp);

        log.info("-----{}", localDateTime);

        SchedulerTaskBo schedulerTaskBo = new SchedulerTaskBo().setScheduleTime(LocalDateTime.now())
                                                               .setTaskState(TaskState.RUNNING)
                                                               .setJobType(JobType.SHELL)
                                                               .setEndTime(null)
                                                               .setCalculationTime(LocalDateTime.now());

        RpcTask rpcTask = schedulerTaskBo.toRpcTask();

        SchedulerTaskBo from = SchedulerTaskBo.from(rpcTask);

        log.info("{}", from.getCalculationTime());
    }

    @Test
    public void map_of_list_thread_safe() {
        Map<Integer, Integer> map = new ConcurrentHashMap<>();

        map.put(1, 1);
        map.put(2, 2);

        Integer integer = map.putIfAbsent(1, 3);
        Integer integer1 = map.computeIfAbsent(3, e -> 3);

        System.out.println(integer);
        System.out.println(integer1);
        System.out.println(map);
    }
}
