package io.github.artiship.arlo.scheduler.manager.dag;

import com.google.common.collect.ImmutableSet;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.scheduler.manager.JobStateStore;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.CronUtils.preScheduleTimeOfSomeTime;

@Slf4j
public class DynamicJobDag extends StaticJobDag {
    private final JobStateStore jobStateStore;

    public DynamicJobDag(final JobStateStore jobStateStore,
                         final SchedulerDao schedulerDao,
                         final TaskScheduler taskScheduler,
                         final TaskDispatcher taskDispatcher,
                         final SchedulerDagBo schedulerDagBo,
                         final SchedulerService schedulerService) {
        super(schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
        this.jobStateStore = jobStateStore;
    }

    @Override
    protected Set<Long> getPredecessorJobIds(Long jobId) {
        return this.jobStateStore.getDependencies(jobId);
    }

    @Override
    protected boolean isPredecessorComplete(Long jobId) {
        Set<Long> dependencies = jobStateStore.getDependencies(jobId);
        if (dependencies.isEmpty()) return false;
        for (Long parentJobId : dependencies) {
            if (parentJobId == -1) continue;
            String jobCron = schedulerDao.getJobCron(jobId);
            String calTimeRangeStr = calTimeRangeStr(preScheduleTimeOfSomeTime(jobCron, schedulerDag.getScheduleTime()), jobCron);

            if (!completedJobSet.contains(parentJobId) && !jobStateStore.isReady(jobId, calTimeRangeStr)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Set<Long> getDagSuccessors(Long predecessorId) {
        Set<Long> successors = schedulerDao.getJobChildren(predecessorId);
        if (successors == null) return ImmutableSet.of();

        return ImmutableSet.copyOf(successors);
    }

    protected Set<Long> getDagPredecessors(Long successorId) {
        return this.jobStateStore.getDependencies(successorId);
    }

    @Override
    public boolean isCompleted() {
        return isCompleted(schedulerDag.getStartId());
    }
}
