package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.AbstractTest;
import io.github.artiship.arlo.scheduler.QuartzConfig;
import io.github.artiship.arlo.scheduler.QuartzSchedulerService;
import io.github.artiship.arlo.scheduler.core.QuartzJob;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class QuartzSchedulerServiceTest extends AbstractTest {

    @Autowired
    private QuartzSchedulerService quartzScheduler;

    @Autowired
    private QuartzConfig quartzConfig;

    private SchedulerJobBo schedulerJob;

    @Before
    public void setup() {
        schedulerJob = new SchedulerJobBo()
                .setJobName("my job")
                .setScheduleCron("0/1 * * * * ?")
                .setWorkerGroups(asList("worker group"));
    }

    @Test
    public void test_standby_scheduler() throws Exception {
        List<String> executions = new ArrayList<>();
        MyQuartzJob.init(executions);
        quartzScheduler.start();

        QuartzSchedulerService standbyScheduler = new QuartzSchedulerService(quartzConfig);
        standbyScheduler.scheduleJob(schedulerJob, MyQuartzJob.class);

        TimeUnit.SECONDS.sleep(10);

        standbyScheduler.removeJob(schedulerJob);
        standbyScheduler.stop();

        assertThat(executions.size()).isGreaterThan(1);
    }

    @Test
    public void schedule_add_remove_job() {
        quartzScheduler.scheduleJob(schedulerJob, MyQuartzJob.class);
        assertThat(quartzScheduler.existsJob(schedulerJob)).isTrue();
        assertThat(quartzScheduler.existsTrigger(schedulerJob)).isTrue();

        //delete from db
        quartzScheduler.removeJob(schedulerJob);
        assertThat(quartzScheduler.existsJob(schedulerJob)).isFalse();
        assertThat(quartzScheduler.existsTrigger(schedulerJob)).isFalse();
    }

    @Test
    public void job_trigger() throws Exception {
        List<String> executions = new ArrayList<>();
        MyQuartzJob.init(executions);
        quartzScheduler.start();
        quartzScheduler.scheduleJob(schedulerJob, MyQuartzJob.class);

        TimeUnit.SECONDS.sleep(10);

        quartzScheduler.removeJob(schedulerJob);
        assertThat(executions.size()).isGreaterThan(1);
    }

    public static class MyQuartzJob implements Job {
        public static List<String> executions;

        public static void init(List<String> history) {
            executions = history;
        }

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            Date nextFireTime = context.getScheduledFireTime();
            SchedulerJobBo job = QuartzJob.getJob(context.getJobDetail());
            log.info("Job {} scheduled at {}", job, nextFireTime);
            executions.add(job.getJobName());
        }
    }
}