package io.github.artiship.arlo.scheduler.rest.service;

import io.github.artiship.arlo.model.ZkLostTask;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.model.enums.DagState;
import io.github.artiship.arlo.model.vo.ComplementDownStreamRequest;
import io.github.artiship.arlo.model.vo.ComplementRequest;
import io.github.artiship.arlo.model.vo.JobRelation;
import io.github.artiship.arlo.scheduler.QuartzSchedulerService;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.core.rpc.RpcClient;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcResponse;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import io.github.artiship.arlo.scheduler.manager.DagScheduler;
import io.github.artiship.arlo.scheduler.manager.JobStateStore;
import io.github.artiship.arlo.scheduler.manager.ResourceManager;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static io.github.artiship.arlo.model.enums.DagNodeType.JOB;
import static io.github.artiship.arlo.model.enums.DagType.STATIC;
import static io.github.artiship.arlo.model.enums.JobPriority.HIGH;
import static io.github.artiship.arlo.model.enums.JobPriority.LOW;
import static io.github.artiship.arlo.model.enums.TaskState.*;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.FAILOVER;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.*;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo.from;
import static io.github.artiship.arlo.scheduler.core.rpc.RpcClient.create;
import static io.github.artiship.arlo.utils.CronUtils.computeScheduleTimesBetween;
import static io.github.artiship.arlo.utils.CronUtils.preScheduleTimeOfSomeTime;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static java.lang.System.currentTimeMillis;
import static java.time.LocalDateTime.now;
import static java.util.Objects.requireNonNull;

@Slf4j
@Service
public class SchedulerService {
    private static final Long NO_TASK_ID = -11L;
    @Autowired
    private QuartzSchedulerService quartzScheduler;
    @Autowired
    private JobStateStore jobStateStore;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private TaskDispatcher taskDispatcher;
    @Autowired
    private TaskScheduler taskScheduler;
    @Autowired
    private DagScheduler dagScheduler;
    @Autowired
    private ResourceManager resourceManager;

    public void scheduleJob(Long jobId) {
        SchedulerJobBo job = schedulerDao.getJobAndDependencies(jobId);
        quartzScheduler.scheduleJob(job);
    }

    public void pauseJob(Long jobId) {
        quartzScheduler.pauseJob(schedulerDao.getJob(jobId));
    }

    public void resumeJob(Long jobId) {
        quartzScheduler.resumeJob(schedulerDao.getJob(jobId));
    }

    public void deleteJob(Long jobId) {
        quartzScheduler.removeJob(schedulerDao.getJob(jobId));
        jobStateStore.removeJob(jobId);
    }

    //run now
    public Long runJob(Long jobId) {
        SchedulerTaskBo task = from(schedulerDao.getJob(jobId)).setTaskTriggerType(MANUAL_RUN);

        return schedulerDao.getDuplicateTask(task.getJobId(), task.getScheduleTime())
                           .orElseGet(() -> {
                               SchedulerTaskBo submit = taskDispatcher.submit(task);
                               return submit.getId();
                           });
    }

    //run at a point in time
    public Long runJob(Long jobId, LocalDateTime pointInTime) {
        if (pointInTime.isAfter(now())) return NO_TASK_ID;

        SchedulerTaskBo task = from(schedulerDao.getJob(jobId));
        LocalDateTime preScheduleTime = preScheduleTimeOfSomeTime(task.getScheduleCron(), pointInTime);

        if (preScheduleTime.isBefore(now())) return NO_TASK_ID;

        task.setTaskTriggerType(MANUAL_RUN)
            .setScheduleTime(preScheduleTime);

        return schedulerDao.getDuplicateTask(task.getJobId(), task.getScheduleTime())
                           .orElseGet(() -> {
                               SchedulerTaskBo submit = taskDispatcher.submit(task);
                               return submit.getId();
                           });
    }

    public void runJob(final ComplementRequest request) {
        log.info("Job_{} complement: start={}, end={}, isCheckDependency={}, parallelism={}",
                request.getJobId(), request.getStartTime(), request.getEndTime(), request.getCheckDependency(), request.getParallelism());

        SchedulerJobBo job = schedulerDao.getJob(request.getJobId());

        Set<LocalDateTime> duplicateScheduleTimes = schedulerDao.getDuplicateScheduleTimes(request.getJobId());

        log.info("Job_{} complement: duplicateScheduleTimes={}", request.getJobId(), duplicateScheduleTimes);

        List<LocalDateTime> scheduleTimes =
                computeScheduleTimesBetween(job.getScheduleCron(), request.getStartTime(), request.getEndTime());

        for (int i = 0; i < scheduleTimes.size(); i++) {
            LocalDateTime scheduleTime = scheduleTimes.get(i);

            SchedulerTaskBo task = from(job).setTaskTriggerType(MANUAL_COMPLEMENT)
                                            .setParallelism(request.getParallelism())
                                            .setJobPriority(LOW)
                                            .setDagId(request.getDagId())
                                            .setScheduleTime(scheduleTime);

            if (request.getParallelism() == 1 && i == 0) {// first doesn't check dependency
                task.setParallelism(0);
            }

            if (scheduleTime.isAfter(now())) {
                log.warn("Job_{} {} {} is before than now and ignore", request.getJobId(), task.getTaskTriggerType(), scheduleTime);
                continue;
            }

            if (duplicateScheduleTimes.contains(scheduleTime)) {
                log.warn("Job_{} {} {} duplicate and ignore", request.getJobId(), task.getTaskTriggerType(), scheduleTime);
                continue;
            }

            if (request.getCheckDependency()) {
                this.taskScheduler.submit(task);
                continue;
            }

            this.taskDispatcher.submit(task);
        }
    }

    public Long rerunTask(Long taskId) {
        SchedulerTaskBo task = schedulerDao.getTask(taskId);

        return schedulerDao.getDuplicateTask(task.getJobId(), task.getScheduleTime())
                           .orElseGet(() -> {
                               SchedulerJobBo job = schedulerDao.getJob(task.getJobId());

                               SchedulerTaskBo submit = taskScheduler.submit(task.toRenewTask()
                                                                                 .setOssPath(job.getOssPath())
                                                                                 .setOffsetMs(job.getOffsetMs())
                                                                                 .setWorkerGroups(job.getWorkerGroups())
                                                                                 .setRetryTimes(0)
                                                                                 .setTaskTriggerType(MANUAL_RERUN)
                                                                                 .setJobPriority(task.getTaskTriggerType() == MANUAL_COMPLEMENT ? LOW : HIGH));

                               log.info("Task_{}_{} CREATED by {}: priority={}, schedule_time={}.",
                                       submit.getJobId(), submit.getId(), submit.getTaskTriggerType(),
                                       submit.getJobPriority(), submit.getScheduleTime());

                               return submit.getId();
                           });


    }


    public boolean markSuccessTask(Long taskId) {
        SchedulerTaskBo task = schedulerDao.getTask(taskId);
        killTask(taskId);

        schedulerDao.updateTaskState(task.getId(), SUCCESS);
        jobStateStore.taskSuccess(task);

        log.info("Task_{}_{} MARKED as success", task.getJobId(), task.getId());
        return true;
    }

    public boolean markFailTask(Long taskId) {
        SchedulerTaskBo task = schedulerDao.getTask(taskId);
        killTask(taskId);

        schedulerDao.updateTaskState(task.getId(), FAIL);
        jobStateStore.taskFailed(task);

        log.info("Task_{}_{} MARKED as fail", task.getJobId(), task.getId());
        return true;
    }

    public boolean killTask(Long taskId) {
        SchedulerTaskBo task = schedulerDao.getTask(taskId);

        if (this.killTask(task)) {
            this.schedulerDao.updateTaskState(task.getId(), KILLED);
            jobStateStore.taskKilled(task);
            this.dagScheduler.stopDag(task);
            return true;
        }
        return false;
    }

    public boolean killTask(SchedulerTaskBo task) {
        if (task.isTerminated()) {
            return true;
        }

        this.schedulerDao.updateTaskState(task.getId(), TO_KILL);

        if (task.getTaskState() == RUNNING) {
            RpcTask rpcTask = task.toRpcTask();
            try (RpcClient rpcClient = create(task.getWorkerHost(), task.getWorkerPort())) {
                log.info("Task_{}_{} SEND to {} to kill", task.getJobId(), task.getId(), task.getWorkerHost());
                RpcResponse response = rpcClient.killTask(rpcTask);

                if (response.getCode() != 200) {
                    log.info("Task_{}_{} SEND to {} kill fail", task.getJobId(), task.getId(), task.getWorkerHost());
                    return false;
                }

                log.info("Task_{}_{} SEND to {} kill success", task.getJobId(), task.getId(), task.getWorkerHost());
                return true;
            } catch (Exception e) {
                log.info("Task_{}_{} SEND to {} kill fail", task.getJobId(), task.getId(), task.getWorkerHost(), e);
            }
        }

        if (task.getTaskState() == PENDING) {
            if (this.taskScheduler.kill(task)) {
                log.info("Task_{}_{} SEND to TaskScheduler kill success", task.getJobId(), task.getId());
                return true;
            }

            if (this.taskDispatcher.kill(task)) {
                log.info("Task_{}_{} SEND to TaskDispatcher kill success", task.getJobId(), task.getId());
                return true;
            }
        }

        if (waitingStates().contains(task.getTaskState())) {
            if (this.taskDispatcher.kill(task)) {
                log.info("Task_{}_{} SEND to TaskDispatcher kill success", task.getJobId(), task.getId());
                return true;
            }
        }

        log.info("Task_{}_{} kill fail", task.getJobId(), task.getId());
        return false;
    }

    public void shutdownWorker(String workerHost) {
        try {
            resourceManager.shutdownWorker(workerHost);
        } catch (Exception e) {
            log.error("Worker_{} shutdown fail", workerHost, e);
        }
    }

    public void resumeWorker(String workerHost) {
        try {
            resourceManager.resumeWorker(workerHost);
        } catch (Exception e) {
            log.error("Worker_{} resume fail", workerHost, e);
        }
    }

    public void updateLostTask(ZkLostTask zkLostTask) {
        log.info("Lost task {} found from zk", zkLostTask);
        if (zkLostTask.getState() == SUCCESS) {
            SchedulerTaskBo task = schedulerDao.saveTask(from(zkLostTask));
            try {
                jobStateStore.taskSuccess(task);
            } catch (Exception e) {
                log.warn("Add success record to job state store fail: taskId={}, id={}", e, task.getId(), task.getJobId());
            }
        } else if (zkLostTask.getState() == FAIL) {
            SchedulerTaskBo task = schedulerDao.getTask(zkLostTask.getId());
            jobStateStore.addFailedHost(task.getJobId(), task.getWorkerHost());
        }
    }

    public void failoverTasks(String workerHost) {
        schedulerDao.getRunningTasksByWorker(workerHost)
                    .forEach(task -> {
                        jobStateStore.taskFailed(task);
                        schedulerDao.updateTaskState(task.getId(), FAIL);
                        log.info("Task_{}_{} failover", task.getJobId(), task.getId());
                        taskDispatcher.submit(task.toRenewTask()
                                                  .setTaskTriggerType(FAILOVER));
                    });
    }

    public void runDag(Long dagId) {
        this.dagScheduler.submit(schedulerDao.getDag(dagId));
    }

    public void runDag(ComplementDownStreamRequest request) {
        log.info("Job_{} complement downStream: start={}, end={}, parallelism={}",
                request.getJobId(), request.getStartTime(), request.getEndTime(), request.getParallelism());

        SchedulerJobBo job = schedulerDao.getJob(request.getJobId());

        List<LocalDateTime> scheduleTimes =
                computeScheduleTimesBetween(job.getScheduleCron(), localDateTime(request.getStartTime()), localDateTime(request.getEndTime()));

        final String batchName = job.getJobName() + "_" + currentTimeMillis();

        for (int i = 0; i < scheduleTimes.size(); i++) {
            LocalDateTime scheduleTime = scheduleTimes.get(i);

            if (scheduleTime.isAfter(now())) {
                log.warn("Dag_{} {} is before than now and ignore", request.getDag(), scheduleTime);
                continue;
            }

            SchedulerDagBo schedulerDagBo = new SchedulerDagBo()
                    .setStartId(request.getJobId())
                    .setScheduleTime(scheduleTime)
                    .setDagName(batchName)
                    .setBatchIndex(i)
                    .setDagType(STATIC)
                    .setDagNodeType(JOB)
                    .setParallelism(request.getParallelism())
                    .setSubmitterId(request.getSubmitterId())
                    .setSubmitterName(request.getSubmitterName())
                    .setDag(request.getDag())
                    .setCreatTime(now())
                    .setUpdateTime(now());

            SchedulerDagBo savedDag = this.schedulerDao.saveDag(schedulerDagBo);
            this.dagScheduler.submit(savedDag);
        }
    }

    public void addJobDependencies(Long jobId, List<JobRelation> relations) {
        requireNonNull(jobId, "Job id is null");

        if (relations == null) return;

        for (JobRelation relation : relations) {
            this.jobStateStore.addJobDependency(relation);
        }
    }

    public void removeJobDependencies(Long jobId, List<JobRelation> relations) {
        requireNonNull(jobId, "Job id is null");

        if (relations == null) return;

        for (JobRelation relation : relations) {
            this.jobStateStore.removeJobDependency(relation);
        }
    }

    public void removeJobDependency(Long jobId, Long parentJobId) {
        this.jobStateStore.removeJobDependency(jobId, parentJobId);
    }

    public Set<Long> getJobDependencies(Long jobId) {
        return this.jobStateStore.getDependencies(jobId);
    }

    public void freeTask(Long taskId) {
        taskScheduler.free(taskId);
    }

    public void stopDag(Long dagId) {
        log.info("Dag_{} to stop", dagId);
        this.dagScheduler.finishDag(dagId, DagState.STOPPED);
    }

    public Set<TaskSuccessRecord> getJobSuccesses(Long jobId) {
        return this.jobStateStore.getJobSuccessHistory(jobId);
    }

    public List<SchedulerNodeBo> getWorkers() {
        return this.resourceManager.getWorkers();
    }

    public Set<Long> getRunningTasks(Long jobId) {
        return this.jobStateStore.getJobRunningTasks(jobId);
    }
}
