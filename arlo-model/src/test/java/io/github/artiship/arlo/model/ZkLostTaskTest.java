package io.github.artiship.arlo.model;

import com.google.gson.Gson;
import io.github.artiship.arlo.model.enums.TaskState;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static io.github.artiship.arlo.model.ZkLostTask.of;
import static java.time.LocalDateTime.now;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ZkLostTaskTest {
    @Test
    public void zk_lost_task_to_json() {
        ZkLostTask lostTask = of(1L, TaskState.SUCCESS, now());
        Gson gson = new Gson();
        String jsonStr = gson.toJson(lostTask);
        ZkLostTask zkLostTask = gson.fromJson(jsonStr, ZkLostTask.class);
        log.info("{}", jsonStr);
        assertEquals(lostTask, zkLostTask);
    }
}