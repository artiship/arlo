package io.github.artiship.arlo.scheduler.manager.dag;

import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.scheduler.manager.JobStateStore;
import io.github.artiship.arlo.scheduler.manager.SchedulerDao;
import io.github.artiship.arlo.scheduler.manager.TaskDispatcher;
import io.github.artiship.arlo.scheduler.manager.TaskScheduler;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicTaskDag extends DynamicJobDag {
    public DynamicTaskDag(final JobStateStore jobStateStore,
                          final SchedulerDao schedulerDao,
                          final TaskScheduler taskScheduler,
                          final TaskDispatcher taskDispatcher,
                          final SchedulerDagBo schedulerDagBo,
                          final SchedulerService schedulerService) {
        super(jobStateStore, schedulerDao, taskScheduler, taskDispatcher, schedulerDagBo, schedulerService);
    }

    @Override
    public void start() {
        process(getStartJobId(), schedulerDag.getScheduleTime());
    }

    @Override
    public boolean isCompleted() {
        return isCompleted(getStartJobId());
    }

    public Long getStartJobId() {
        return schedulerDao.getJobIdByTaskId(schedulerDag.getStartId())
                           .get();
    }
}
