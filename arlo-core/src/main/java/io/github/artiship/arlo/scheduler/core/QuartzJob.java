package io.github.artiship.arlo.scheduler.core;

import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;

import static io.github.artiship.arlo.model.enums.TaskTriggerType.CRON;
import static io.github.artiship.arlo.utils.Dates.localDateTime;

@Slf4j
public class QuartzJob implements Job {
    public static final String DATA_KEY = "data";
    private static SchedulerService schedulerService;

    public static void init(SchedulerService schedulerService) {
        QuartzJob.schedulerService = schedulerService;
    }

    public static SchedulerJobBo getJob(JobDetail jobDetail) {
        return (SchedulerJobBo) jobDetail.getJobDataMap()
                                         .get(DATA_KEY);
    }

    @Override
    public void execute(JobExecutionContext context) {
        schedulerService.submit(SchedulerTaskBo.from(getJob(context.getJobDetail()))
                                               .setTaskTriggerType(CRON)
                                               .setScheduleTime(localDateTime(context.getScheduledFireTime())));
    }
}
