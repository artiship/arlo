package io.github.artiship.arlo.scheduler.manager.dag;

import com.google.common.collect.ImmutableSet;
import com.google.common.graph.MutableGraph;
import io.github.artiship.arlo.model.bo.DagNode;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.model.enums.JobPriority;
import io.github.artiship.arlo.model.enums.TaskTriggerType;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.manager.dependency.DagDependencyBuilder;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.graph.GraphBuilder.directed;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo.from;
import static java.util.stream.Collectors.toSet;

@Slf4j
public class StaticJobDag extends AbstractDag {

    private final TaskDispatcher taskDispatcher;
    private final SchedulerService schedulerService;
    protected MutableGraph<Long> graph = directed().build();
    private SchedulerJobBo startJob;

    public StaticJobDag(final SchedulerDao schedulerDao,
                        final TaskScheduler taskScheduler,
                        final TaskDispatcher taskDispatcher,
                        final SchedulerDagBo schedulerDagBo,
                        final SchedulerService schedulerService) {
        super(schedulerDagBo, schedulerDao, taskScheduler);
        this.schedulerService = schedulerService;
        this.taskDispatcher = taskDispatcher;
    }

    @Override
    public void start() {
        if (schedulerDag.getDag() != null) {
            for (DagNode node : schedulerDag.getDag()) {
                Long parentJobId = node.getId();
                for (Long childId : node.getChildren()) {
                    graph.putEdge(parentJobId, childId);
                }
            }
        }

        log.info("Dag_{} start: dag={} ", schedulerDag.getId(), graph);

        startJob = schedulerDao.getJob(schedulerDag.getStartId());

        process(schedulerDag.getStartId(), schedulerDag.getScheduleTime());
    }

    public boolean shouldIgnore(Long jobId) {
        if (jobId == null) return false;

        for (DagNode node : schedulerDag.getDag()) {
            if (jobId.equals(node.getId()) && node.isIgnore()) {
                log.info("Dag_{} IGNORE job_{}", this.schedulerDag.getId(), jobId);
                return true;
            }
        }

        return false;
    }

    @Override
    public void advance(SchedulerTaskBo task) {
        this.handleAdvance(task);

        getReadySuccessors().forEach(jobId -> {
            if (shouldIgnore(jobId)) {
                advance(new SchedulerTaskBo().setDagId(this.schedulerDag.getId())
                                             .setJobId(jobId));
                return;
            }

            childScheduleTime(jobId).ifPresent(childScheduleTime -> {
                if (schedulerDao.dagHasJobSuccessOrUnfinished(schedulerDag.getId(), jobId, childScheduleTime)) {
                    advance(new SchedulerTaskBo().setDagId(this.schedulerDag.getId())
                                                 .setJobId(jobId));
                    return;
                }

                SchedulerTaskBo taskToRun = from(schedulerDao.getJob(jobId));

                taskToRun.setScheduleTime(childScheduleTime)
                         .setTaskTriggerType(TaskTriggerType.MANUAL_COMPLEMENT_DOWNSTREAM)
                         .setDagId(schedulerDag.getId())
                         .setJobPriority(JobPriority.LOW)
                         .addSkipDecencies(getDagPredecessors(jobId));

                schedulerDao.getDuplicateTask(task.getJobId(), task.getScheduleTime())
                            .ifPresent(taskId -> {
                                schedulerService.killTask(taskId);
                            });

                taskDispatcher.submit(taskToRun);
                log.info("Dag_{} submit task for job {}", schedulerDag.getId(), taskToRun.getJobId());
            });
        });
    }

    protected Set<Long> getDagSuccessors(Long predecessorId) {
        Set<Long> successors = graph.successors(predecessorId);

        if (successors == null) return ImmutableSet.of();

        return ImmutableSet.copyOf(successors);
    }

    protected Set<Long> getDagPredecessors(Long successorId) {
        Set<Long> predecessors = graph.predecessors(successorId);

        if (predecessors == null) return ImmutableSet.of();

        return ImmutableSet.copyOf(predecessors);
    }

    @Override
    public Set<Long> getReadySuccessors() {
        return getDagSuccessors(lastCompleted).stream()
                                              .filter(this::isPredecessorComplete)
                                              .collect(toSet());
    }

    protected boolean isPredecessorComplete(Long successorId) {
        for (Long jobId : getPredecessorJobIds(successorId)) {
            if (!this.completedJobSet.contains(jobId)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Set<Long> getPredecessorJobIds(Long jobId) {
        return this.getDagPredecessors(jobId);
    }

    @Override
    protected void process(Long jobId, LocalDateTime scheduleTime) {
        log.info("Dag_{} PROCESS job_{} {}", schedulerDag.getId(), jobId, scheduleTime);
        if (isStop()) return;
        if (jobId == -1L) return;
        if (!shouldIgnore(jobId) && !schedulerDao.dagHasJobSuccessOrUnfinished(schedulerDag.getId(), jobId, scheduleTime)) {
            log.info("Dag_{} SUBMIT job_{} {}", schedulerDag.getId(), jobId, scheduleTime);

            taskDispatcher.submit(SchedulerTaskBo.from(schedulerDao.getJob(jobId))
                                                 .setTaskTriggerType(TaskTriggerType.MANUAL_RUN)
                                                 .setDagId(schedulerDag.getId())
                                                 .setJobPriority(JobPriority.LOW)
                                                 .setScheduleTime(scheduleTime));
            return;
        }

        lastCompleted = jobId;
        completedJobSet.add(jobId);
        log.info("Dag_{} SKIP job_{} {}.", schedulerDag.getId(), jobId, scheduleTime);

        getDagSuccessors(lastCompleted)
                .forEach(childJobId ->
                        childScheduleTime(childJobId).ifPresent(childScheduleTime -> process(childJobId, childScheduleTime)));
    }

    private Optional<LocalDateTime> childScheduleTime(Long childJobId) {
        DagDependencyBuilder builder = DagDependencyBuilder.builder()
                                                           .parentCronExpression(startJob.getScheduleCron())
                                                           .parentScheduleTime(schedulerDag.getScheduleTime())
                                                           .childCronExpression(schedulerDao.getJobCron(childJobId))
                                                           .build();
        log.info("{}", builder);

        List<LocalDateTime> schedulerTimes = builder.childSchedulerTimes();

        if (schedulerTimes == null && schedulerTimes.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(schedulerTimes.get(0));
    }

    @Override
    public boolean isCompleted() {
        log.info("Dag_{} check complete: current={}", schedulerDag.getId(), completedJobSet);
        return isCompleted(schedulerDag.getStartId());
    }

    protected boolean isCompleted(Long jobId) {
        if (!completedJobSet.contains(jobId))
            return false;

        for (Long successorJobId : getDagSuccessors(jobId)) {
            if (!isCompleted(successorJobId))
                return false;
        }

        return true;
    }
}
