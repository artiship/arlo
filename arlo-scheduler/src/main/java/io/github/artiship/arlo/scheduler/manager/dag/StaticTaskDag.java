package io.github.artiship.arlo.scheduler.manager.dag;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.github.artiship.arlo.model.bo.DagNode;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.Set;

import static io.github.artiship.arlo.model.enums.JobPriority.LOW;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_RERUN;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_RUN;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo.from;
import static io.github.artiship.arlo.utils.CronUtils.preScheduleTimeOfSomeTime;
import static java.util.stream.Collectors.toSet;

@Slf4j
public class StaticTaskDag extends StaticJobDag {

    private BiMap<Long, Long> taskJobMap = HashBiMap.create();

    public StaticTaskDag(final SchedulerDao schedulerDao,
                         final TaskScheduler taskScheduler,
                         final TaskDispatcher taskDispatcher,
                         final SchedulerDagBo schedulerDagBo,
                         final SchedulerService schedulerService) {
        super(schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
    }

    @Override
    public void start() {
        if (schedulerDag.getDag() != null) {
            for (DagNode node : schedulerDag.getDag()) {
                taskJobMap.put(node.getId(), schedulerDao.getJobIdByTaskId(node.getId())
                                                         .get());
                for (Long childTaskId : node.getChildren()) {
                    graph.putEdge(node.getId(), childTaskId);
                }
            }
        }

        log.info("Dag_{} start: dag={} ", schedulerDag.getId(), graph);

        process(schedulerDag.getStartId(), schedulerDag.getScheduleTime());
    }

    @Override
    public void advance(SchedulerTaskBo task) {
        this.handleAdvance(task);

        getReadySuccessors().forEach(taskId -> {
            Long jobId = this.taskJobMap.get(taskId);
            SchedulerTaskBo taskToRun = from(schedulerDao.getJob(jobId));
            taskToRun.setScheduleTime(preScheduleTimeOfSomeTime(task.getScheduleCron(), schedulerDag.getScheduleTime()))
                     .setDagId(this.schedulerDag.getId())
                     .setTaskTriggerType(MANUAL_RERUN)
                     .setJobPriority(LOW)
                     .addSkipDecencies(getPredecessorJobIds(taskId));
            taskScheduler.submit(taskToRun);

            log.info("Dag_{} SUBMIT job {}",
                    schedulerDag.getId(), taskToRun.getJobId());
        });
    }

    @Override
    public Set<Long> getReadySuccessors() {
        return getDagSuccessors(this.taskJobMap.inverse()
                                               .get(lastCompleted))
                .stream()
                .filter(taskId -> isPredecessorComplete(taskId))
                .collect(toSet());
    }

    @Override
    protected boolean isPredecessorComplete(Long successorId) {
        for (Long jobId : getPredecessorJobIds(successorId)) {
            if (!this.completedJobSet.contains(jobId)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Set<Long> getPredecessorJobIds(Long successorId) {
        return this.getDagPredecessors(successorId)
                   .stream()
                   .map(taskJobMap::get)
                   .collect(toSet());
    }

    @Override
    protected void process(Long taskId, LocalDateTime scheduleTime) {
        Long jobId = this.taskJobMap.get(taskId);

        log.info("Dag_{} PROCESS job_{} {}", schedulerDag.getId(), jobId, scheduleTime);

        if (!schedulerDao.dagHasJobSuccessOrUnfinished(schedulerDag.getId(), jobId, scheduleTime)) {
            log.info("Dag_{} SUBMIT job_{} {}", schedulerDag.getId(), jobId, scheduleTime);

            taskScheduler.submit(schedulerDao.getTask(taskId)
                                             .toRenewTask()
                                             .setDagId(schedulerDag.getId())
                                             .setTaskTriggerType(MANUAL_RUN)
                                             .setJobPriority(LOW)
                                             .setScheduleTime(scheduleTime));
            return;
        }

        lastCompleted = jobId;
        completedJobSet.add(jobId);
        log.info("Dag_{} SKIP job_{} {}.", schedulerDag.getId(), jobId, scheduleTime);

        getDagSuccessors(taskId)
                .forEach(id -> {
                    SchedulerTaskBo task = schedulerDao.getTask(id);
                    process(id, preScheduleTimeOfSomeTime(task.getScheduleCron(), schedulerDag.getScheduleTime()));
                });
    }

    @Override
    public boolean isCompleted() {
        log.info("Dag_{} check complete: current={}", schedulerDag.getId(), completedJobSet);
        return isCompleted(schedulerDag.getStartId());
    }

    @Override
    protected boolean isCompleted(Long id) {
        if (!completedJobSet.contains(this.taskJobMap.get(id)))
            return false;

        for (Long taskId : getDagSuccessors(id)) {
            if (!isCompleted(taskId))
                return false;
        }

        return true;
    }
}
