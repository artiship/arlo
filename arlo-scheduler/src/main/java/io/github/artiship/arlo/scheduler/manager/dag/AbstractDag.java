package io.github.artiship.arlo.scheduler.manager.dag;

import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public abstract class AbstractDag implements GenericDag {

    protected final SchedulerDagBo schedulerDag;
    protected final SchedulerDao schedulerDao;
    protected final TaskScheduler taskScheduler;
    protected final Set<Long> completedJobSet = new HashSet<>();
    protected long lastCompleted = -1;
    private boolean isStop = false;

    protected AbstractDag(SchedulerDagBo schedulerDag, SchedulerDao schedulerDao, TaskScheduler taskScheduler) {
        this.schedulerDag = schedulerDag;
        this.schedulerDao = schedulerDao;
        this.taskScheduler = taskScheduler;
    }

    public void handleAdvance(final SchedulerTaskBo task) {
        if (isStop()) return;
        lastCompleted = task.getJobId();
        completedJobSet.add(task.getJobId());
        log.info("Dag_{} SUCCESS node_{} of job_{} ", schedulerDag.getId(), lastCompleted, task.getJobId());
    }

    protected abstract Set<Long> getReadySuccessors();

    protected abstract Set<Long> getPredecessorJobIds(Long id);

    protected abstract void process(Long id, LocalDateTime scheduleTime);

    @Override
    public Long getId() {
        return schedulerDag.getId();
    }

    @Override
    public String getName() {
        return schedulerDag.getDagName();
    }

    @Override
    public void stop() {
        this.isStop = true;
    }

    protected boolean isStop() {
        return isStop;
    }
}
