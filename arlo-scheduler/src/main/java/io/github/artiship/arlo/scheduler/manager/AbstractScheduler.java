package io.github.artiship.arlo.scheduler.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.artiship.arlo.scheduler.core.SchedulerService;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import static io.github.artiship.arlo.model.enums.TaskState.KILLED;
import static java.time.LocalDateTime.now;
import static java.util.concurrent.TimeUnit.HOURS;

@Slf4j
public abstract class AbstractScheduler implements SchedulerService {

    protected ThreadPoolExecutor threadPool;

    protected Cache<Long, LocalDateTime> toKillTasks = CacheBuilder.newBuilder()
                                                                   .expireAfterWrite(1, HOURS)
                                                                   .build();

    @Override
    public boolean kill(SchedulerTaskBo task) {
        if (this.threadPool == null) return false;

        toKillTasks.put(task.getId(), task.getScheduleTime());
        return true;
    }

    protected boolean tryKill(final SchedulerDao schedulerDao, final SchedulerTaskBo task) {
        if (toKillTasks.getIfPresent(task.getId()) != null) {
            schedulerDao.saveTask(task.setTaskState(KILLED)
                                      .setEndTime(now()));
            toKillTasks.invalidate(task.getId());

            log.info("Task_{}_{} has been killed", task.getJobId(), task.getId());
            return true;
        }

        return false;
    }

    public long queuedTaskCount() {
        if (threadPool == null) return 0;

        return this.threadPool.getQueue()
                              .size();
    }

    @Override
    public void stop() throws Exception {
        if (this.threadPool == null || this.threadPool.isShutdown())
            return;

        this.threadPool.shutdownNow();
    }

    protected ThreadFactory createThreadFactory(String name) {
        return new ThreadFactoryBuilder().setNameFormat(name + "-%d")
                                         .build();
    }
}
