package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.core.exception.TaskNotFoundException;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.RpcClient;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.utils.Dates;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.artiship.arlo.model.enums.JobType.MYSQL2HIVE;
import static io.github.artiship.arlo.model.enums.TaskState.*;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_COMPLEMENT;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_COMPLEMENT_DOWNSTREAM;
import static io.github.artiship.arlo.scheduler.core.rpc.RpcClient.create;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
@Component
public class TaskDispatcher extends AbstractScheduler {
    @Value("${services.task-dispatcher.thread.count:2}")
    private int threadCount;
    @Value("${services.task-dispatcher.extract.limit.enable:false}")
    private boolean extractLimitEnable;
    @Value("${services.task-dispatcher.extract.limit:8}")
    private int extractLimit;

    @Autowired
    private ResourceManager resourceManager;
    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;

    private ReentrantLock jobConcurrencyLock = new ReentrantLock();

    private ThreadPoolExecutor limitedThreadPool;

    @Override
    public SchedulerTaskBo submit(SchedulerTaskBo task) {
        SchedulerTaskBo taskUpdated = this.schedulerDao.saveTask(task.setTaskState(WAITING)
                                                                     .setWaitingTime(now()));

        try {
            jobStateStore.removeTaskSuccessRecord(taskUpdated);
        } catch (Exception e) {
            log.info("Task_{}_{} SUBMIT to task dispatcher remove success records fail,",
                    taskUpdated.getJobId(), taskUpdated.getId(), e);
            return this.schedulerDao.saveTask(task.setTaskState(FAIL)
                                                  .setWaitingTime(now()));
        }

        this.threadPool.execute(new DispatchTaskThread(taskUpdated));

        log.info("Task_{}_{} SUBMIT to task dispatcher: {}, {}, {}",
                taskUpdated.getJobId(), taskUpdated.getId(),
                taskUpdated.getTaskTriggerType(), taskUpdated.getJobPriority(), taskUpdated.getScheduleTime());

        return taskUpdated;
    }

    @Override
    public void start() {
        this.threadPool = new ThreadPoolExecutor(threadCount, threadCount, 0L, MILLISECONDS,
                new PriorityBlockingQueue<>(), createThreadFactory("Task-dispatcher-cron"), new DiscardPolicy());

        this.limitedThreadPool = new ThreadPoolExecutor(threadCount, threadCount, 0L, MILLISECONDS,
                new LinkedBlockingQueue<>(), createThreadFactory("Task-dispatcher-limited"), new DiscardPolicy());

        log.info("Reload waiting tasks to dispatcher - Start...");
        this.schedulerDao.getTasksByState(WAITING,
                WAITING_PARALLELISM_LIMIT,
                WAITING_TASK_SLOT,
                WAITING_COMPLEMENT_LIMIT,
                WAITING_EXTRACT_LIMIT)
                         .forEach(t -> {
                             this.threadPool.execute(new DispatchTaskThread(t));
                             log.info("Task_{}_{} RELOAD to task dispatcher: {}, {}, {}",
                                     t.getJobId(), t.getId(), t.getTaskTriggerType(), t.getJobPriority(), t.getScheduleTime());
                         });
        log.info("Reload waiting tasks to dispatcher - Stopped");
    }

    private void updateTaskState(SchedulerTaskBo task, TaskState state) {
        if (task.getTaskState() != state) {
            schedulerDao.updateTaskState(task.getId(), state);
            task.setTaskState(state);
        }
    }

    public long limitedQueuedTaskCount() {
        if (limitedThreadPool == null) return 0;

        return limitedThreadPool.getQueue()
                                .size();
    }

    private class DispatchTaskThread implements Runnable, Comparable<DispatchTaskThread> {

        private SchedulerTaskBo task;

        public DispatchTaskThread(SchedulerTaskBo task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                if (tryKill(schedulerDao, task)) return;
                if (dispatch()) return;
            } catch (Exception e) {
                log.error("Task_{}_{} dispatch cause exception", task.getJobId(), task.getId(), e);
            }

            task.penalty();

            log.debug("Task_{}_{}: {}, {}, {}",
                    task.getJobId(), task.getId(), task.getJobPriority(), task.getPenalty(), task.getScheduleTime());

            getExecutor(task.getTaskState()).execute(this);
        }

        private ThreadPoolExecutor getExecutor(TaskState taskState) {
            if (asList(WAITING_COMPLEMENT_LIMIT, WAITING_EXTRACT_LIMIT, WAITING_PARALLELISM_LIMIT).contains(taskState))
                return limitedThreadPool;
            return threadPool;
        }

        private boolean dispatch() {
            if (asList(MANUAL_COMPLEMENT, MANUAL_COMPLEMENT_DOWNSTREAM).contains(task.getTaskTriggerType())
                    && now().getHour() < 10) {
                updateTaskState(task, WAITING_COMPLEMENT_LIMIT);
                return false;
            }

            jobConcurrencyLock.lock();
            try {
                if (extractLimitEnable && task.getJobType() == MYSQL2HIVE
                        && jobStateStore.extractLimit(task.getSourceHost(), extractLimit)) {
                    updateTaskState(task, WAITING_EXTRACT_LIMIT);
                    return false;
                }


                if (jobStateStore.exceedConcurrencyLimit(task.getJobId(), task.parallelism())) {
                    updateTaskState(task, WAITING_PARALLELISM_LIMIT);
                    return false;
                }

                if (task.parallelism() == 1 && !jobStateStore.isPreReady(task)) {
                    updateTaskState(task, WAITING_PARALLELISM_LIMIT);
                    return false;
                }

                final Optional<SchedulerNodeBo> availableWorker = resourceManager.availableWorker(task);

                if (!availableWorker.isPresent()) {
                    updateTaskState(task, WAITING_TASK_SLOT);
                    return false;
                }

                SchedulerNodeBo worker = availableWorker.get();

                try (final RpcClient workerRpcClient = create(worker.getHost(), worker.getPort())) {
                    final RpcTask rpcTask = task.setWorkerHost(worker.getHost())
                                                .setWorkerPort(worker.getPort())
                                                .toRpcTask();

                    workerRpcClient.submitTask(rpcTask);

                    log.info("Task_{}_{} DISPATCH to {}: priority={}, penalty={}, schedule_time={}, cal_time{}",
                            task.getJobId(), task.getId(), worker.getHost(),
                            task.getJobPriority(), task.getPenalty(), task.getScheduleTime(), Dates.localDateTime(rpcTask.getCalculationTime()));
                } catch (StatusRuntimeException e) {
                    resourceManager.unHealthyWorker(worker.getHost());
                    log.warn("Worker_{} can't reach, add to un healthy check", worker.getHost(), e);
                    return false;
                }

                try {
                    jobStateStore.addJobRunningTask(task);
                    resourceManager.decrease(worker.getHost());

                    schedulerDao.saveTask(task.setTaskState(DISPATCHED)
                                              .setDispatchedTime(now()));
                } catch (Exception e) {
                    log.info("Task_{}_{} update state after dispatched fail", task.getJobId(), task.getId(), e);
                }

                return true;
            } catch (TaskNotFoundException e) {
                log.warn("Task_{}_{} has already deleted.", task.getJobId(), task.getId(), e);
                return true;
            } catch (Exception e) {
                log.error("Task_{}_{} dispatch failed.", task.getJobId(), task.getId(), e);
            } finally {
                jobConcurrencyLock.unlock();
            }

            return false;
        }

        public SchedulerTaskBo getTask() {
            return task;
        }

        @Override
        public int compareTo(DispatchTaskThread o) {
            return this.task.compareTo(o.getTask());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DispatchTaskThread that = (DispatchTaskThread) o;
            return Objects.equals(task, that.task);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task);
        }
    }
}