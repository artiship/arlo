package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.enums.TaskTriggerType;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.github.artiship.arlo.model.enums.JobPriority.*;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TaskSchedulerTest {

    private LinkedBlockingQueue<SchedulerTaskBo> queue = new LinkedBlockingQueue<>();

    @Test
    public void priority_queue() throws InterruptedException {
        SchedulerTaskBo t1 = new SchedulerTaskBo().setJobPriority(LOW);
        SchedulerTaskBo t2 = new SchedulerTaskBo().setJobPriority(MEDIUM);
        SchedulerTaskBo t3 = new SchedulerTaskBo().setJobPriority(HIGH);
        SchedulerTaskBo t4 = new SchedulerTaskBo().setJobPriority(HIGHEST);

        queue.add(t4);
        queue.add(t3);
        queue.add(t2);
        queue.add(t1);

        ExecutorService executorService = newFixedThreadPool(10);
        executorService.submit(() -> {
            while (queue.size() > 0) {
                SchedulerTaskBo task = null;
                try {
                    task = queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("{}", task.getJobPriority());
                queue.offer(task);
            }
        });

        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void seven_days_crated_cron_task_should_kill() {
        SchedulerTaskBo task7Days = new SchedulerTaskBo().setTaskTriggerType(TaskTriggerType.CRON)
                                                         .setCreateTime(LocalDateTime.now()
                                                                                     .minusDays(7));

        SchedulerTaskBo task6Days = new SchedulerTaskBo().setTaskTriggerType(TaskTriggerType.CRON)
                                                         .setCreateTime(LocalDateTime.now()
                                                                                     .minusDays(6));

        TaskScheduler taskScheduler = new TaskScheduler();
        assertThat(taskScheduler.isAbandoned(task7Days)).isTrue();
        assertThat(taskScheduler.isAbandoned(task6Days)).isFalse();
    }
}