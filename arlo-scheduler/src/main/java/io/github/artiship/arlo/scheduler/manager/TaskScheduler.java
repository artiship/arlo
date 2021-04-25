package io.github.artiship.arlo.scheduler.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.artiship.arlo.scheduler.core.exception.CronNotSatisfiedException;
import io.github.artiship.arlo.scheduler.core.exception.JobNotFoundException;
import io.github.artiship.arlo.scheduler.core.exception.TaskNotFoundException;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.artiship.arlo.model.enums.TaskState.*;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.CRON;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_FREE;
import static io.github.artiship.arlo.scheduler.core.QuartzJob.init;
import static java.time.Duration.between;
import static java.time.LocalDateTime.now;
import static java.util.concurrent.TimeUnit.HOURS;

@Slf4j
@Component
public class TaskScheduler extends AbstractScheduler {
    @Value("${services.task-scheduler.thread.count:2}")
    private int threadCount;

    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskDispatcher taskDispatcher;

    private Cache<Long, Long> toFreeTasks = CacheBuilder.newBuilder()
                                                        .expireAfterWrite(1, HOURS)
                                                        .build();

    @Override
    public SchedulerTaskBo submit(SchedulerTaskBo task) {
        SchedulerTaskBo taskUpdated = schedulerDao.saveTask(task.setTaskState(PENDING)
                                                                .setPendingTime(now()));
        taskUpdated.setSkipDependencies(task.getSkipDependencies());

        try {
            jobStateStore.removeTaskSuccessRecord(taskUpdated);
        } catch (Exception e) {
            log.info("Task_{}_{} SUBMIT to task scheduler remove success records fail,",
                    taskUpdated.getJobId(), taskUpdated.getId(), e);
            return this.schedulerDao.saveTask(task.setTaskState(FAIL)
                                                  .setWaitingTime(now()));
        }

        this.threadPool.execute(new ScheduleTaskThread(taskUpdated));

        log.info("Task_{}_{} SUBMIT to task scheduler: {}, {}, {}",
                taskUpdated.getJobId(), taskUpdated.getId(),
                taskUpdated.getTaskTriggerType(), taskUpdated.getJobPriority(), taskUpdated.getScheduleTime());

        return taskUpdated;
    }

    public void free(Long taskId) {
        this.toFreeTasks.put(taskId, taskId);
    }

    protected boolean tryFree(final SchedulerTaskBo task) {
        if (toFreeTasks.getIfPresent(task.getId()) != null) {
            schedulerDao.saveTask(task.setTaskTriggerType(MANUAL_FREE));
            toKillTasks.invalidate(task.getId());

            log.info("Task_{}_{} FREE dependencies", task.getJobId(), task.getId());
            return true;
        }

        return false;
    }

    @Override
    public void start() {
        init(this);

        threadPool = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("Task-scheduler-%d")
                                                                       .build(),
                new ThreadPoolExecutor.DiscardPolicy());


        log.info("Load left pending tasks into task scheduler");
        schedulerDao.getTasksByState(PENDING)
                    .forEach(t -> {
                        threadPool.submit(new ScheduleTaskThread(t));
                        log.info("Task_{}_{} load and resubmit to task scheduler.", t.getJobId(), t.getId());
                    });
    }

    public void propagate(Set<Long> jobIds) {

    }

    public boolean isAbandoned(final SchedulerTaskBo task) {
        return task.getTaskTriggerType() == CRON && between(task.getCreateTime(), now()).toDays() >= 7;
    }

    private class ScheduleTaskThread implements Runnable {
        private SchedulerTaskBo task;

        public ScheduleTaskThread(SchedulerTaskBo task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                if (tryKill(schedulerDao, task)) return;

                if (isAbandoned(task)) {
                    schedulerDao.saveTask(task.setTaskState(KILLED)
                                              .setEndTime(now()));
                    log.warn("Task_{}_{} is killed because of abandon.", task.getJobId(), task.getId());
                    return;
                }

                if (tryFree(task) || jobStateStore.isDependencyReady(task)) {
                    taskDispatcher.submit(task);
                    return;
                }
            } catch (TaskNotFoundException e) {
                log.warn("Task_{}_{} has already deleted.", task.getJobId(), task.getId(), e);
                return;
            } catch (JobNotFoundException | CronNotSatisfiedException e) {
                log.error("Task_{}_{} check dependencies fail: schedule time {}",
                        task.getJobId(), task.getId(), task.getScheduleTime(), e);

                schedulerDao.saveTask(task.setEndTime(now())
                                          .setTaskState(FAIL));
                return;
            } catch (Exception e) {
                log.error("Task_{}_{} check dependencies fail: schedule time {}",
                        task.getJobId(), task.getId(), task.getScheduleTime(), e);
            }

            threadPool.execute(this);
        }

        @Override
        public boolean equals(Object o) {
            return this.task.equals(o);
        }

        @Override
        public int hashCode() {
            return task.hashCode();
        }
    }
}
