package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.enums.JobPriority;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

import static io.github.artiship.arlo.model.enums.JobPriority.*;
import static java.time.LocalDateTime.now;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TaskDispatcherTest {
    final PriorityBlockingQueue<SchedulerTaskBo> waitingQueue = new PriorityBlockingQueue<>();
    SchedulerTaskBo low = new SchedulerTaskBo().setJobPriority(JobPriority.LOW);
    SchedulerTaskBo low_2 = new SchedulerTaskBo().setJobPriority(JobPriority.LOW);
    SchedulerTaskBo medium = new SchedulerTaskBo().setJobPriority(MEDIUM);
    SchedulerTaskBo high = new SchedulerTaskBo().setJobPriority(JobPriority.HIGH);

    @Test
    public void task_penalty() throws InterruptedException {
        waitingQueue.add(low);
        waitingQueue.add(medium);
        waitingQueue.add(high);

        List<JobPriority> list = new ArrayList<>();
        while (waitingQueue.iterator()
                           .hasNext()) {
            list.add(waitingQueue.take()
                                 .getJobPriority());
        }

        assertThat(list).isEqualTo(Arrays.asList(HIGH, MEDIUM, LOW));

        waitingQueue.add(high.penalty());
        waitingQueue.add(low);
        waitingQueue.add(medium);
        list.clear();
        while (waitingQueue.iterator()
                           .hasNext()) {
            list.add(waitingQueue.take()
                                 .getJobPriority());
        }

        //penalty task will add to the tail of the queue
        assertThat(list).isEqualTo(Arrays.asList(MEDIUM, LOW, HIGH));
    }

    @Test
    public void schedule_time_sorting() throws InterruptedException {
        LocalDateTime before = now().minusDays(1);
        LocalDateTime after = now();

        waitingQueue.add(high.penalty());
        waitingQueue.add(low.setScheduleTime(after));
        waitingQueue.add(low_2.setScheduleTime(before));
        waitingQueue.add(medium);

        List<SchedulerTaskBo> tasks = new ArrayList<>();
        while (waitingQueue.iterator()
                           .hasNext()) {
            tasks.add(waitingQueue.take());
        }

        //penalty task will add to the tail of the queue
        //smaller schedule time should run first
        assertThat(tasks.stream()
                        .map(i -> i.getJobPriority())
                        .collect(Collectors.toList())).isEqualTo(Arrays.asList(MEDIUM, LOW, LOW, HIGH));
        assertThat(tasks.get(1)
                        .getScheduleTime()).isEqualTo(before);
        assertThat(tasks.get(2)
                        .getScheduleTime()).isEqualTo(after);
    }
}