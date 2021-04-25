package io.github.artiship.arlo.scheduler;

import io.github.artiship.arlo.model.exception.ArloRuntimeException;
import io.github.artiship.arlo.scheduler.core.QuartzJob;
import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import static java.util.Objects.requireNonNull;

@Slf4j
public class QuartzSchedulerService implements Service {
    private Scheduler scheduler;

    public QuartzSchedulerService(QuartzConfig quartzConfig) {
        try {
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(quartzConfig);
            scheduler = schedulerFactory.getScheduler();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    @Override
    public void start() throws Exception {
        scheduler.start();
    }

    public void scheduleJob(SchedulerJobBo job) {
        scheduleJob(job, QuartzJob.class, false);
    }

    public void scheduleJobWithCompleteMissingPoints(SchedulerJobBo job, Boolean completeMissingPoints) {
        scheduleJob(job, QuartzJob.class, completeMissingPoints);
    }

    public void scheduleJob(SchedulerJobBo job, Class<? extends Job> jobClass) {
        this.scheduleJob(job, jobClass, false);
    }

    public void scheduleJob(SchedulerJobBo job, Class<? extends Job> jobClass, Boolean completeMissingPoints) {
        try {
            if (completeMissingPoints == null) completeMissingPoints = false;
            final TriggerKey triggerKey = job.getTriggerKey();
            final CronTrigger cronTrigger = job.getCronTrigger();
            if (scheduler.checkExists(triggerKey)) {
                scheduler.addJob(job.getJobDetail(jobClass), true);
                log.info("Job_{} cron {} added", job.getId(), job.getScheduleCron());

                String oldCronExpression = ((CronTrigger) scheduler.getTrigger(triggerKey)).getCronExpression();
                String newCronExpression = cronTrigger.getCronExpression();

                if (completeMissingPoints && newCronExpression.equalsIgnoreCase(oldCronExpression)) {
                    scheduler.resumeJob(job.getJobKey());
                    log.info("Job_{} cron {} resumed", job.getId(), job.getScheduleCron());
                    return;
                }

                scheduler.rescheduleJob(triggerKey, job.getCronTrigger());
                log.info("Job_{} cron {} rescheduled", job.getId(), job.getScheduleCron());
                return;
            }

            scheduler.scheduleJob(job.getJobDetail(jobClass), job.getCronTrigger());
            log.info("Job_{} cron {} scheduled", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            log.error("Job_{} cron {} scheduleJob fail", job.getId(), job.getScheduleCron(), e);
            throw new ArloRuntimeException(e);
        }
    }

    public void pauseJob(SchedulerJobBo job) {
        requireNonNull(job, "Job is null");

        try {
            JobKey jobKey = job.getJobKey();
            TriggerKey triggerKey = job.getTriggerKey();
            if (scheduler.checkExists(triggerKey)) {
                scheduler.pauseJob(jobKey);
            }

            log.info("Job_{} cron {} paused", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new ArloRuntimeException(e);
        }
    }


    public void resumeJob(SchedulerJobBo job) {
        requireNonNull(job, "Job is null");

        try {
            JobKey jobKey = job.getJobKey();
            TriggerKey triggerKey = job.getTriggerKey();
            if (scheduler.checkExists(triggerKey)) {
                scheduler.resumeJob(jobKey);
            }
            log.info("Job_{} cron {} resumed", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new ArloRuntimeException(e);
        }
    }

    public void removeJob(SchedulerJobBo job) {
        requireNonNull(job, "Job is null");

        try {
            TriggerKey triggerKey = job.getTriggerKey();
            JobKey jobKey = job.getJobKey();
            if (scheduler.checkExists(jobKey)) {
                scheduler.pauseTrigger(triggerKey);
                scheduler.unscheduleJob(triggerKey);
                scheduler.deleteJob(jobKey);
            }
            log.info("Job_{} cron {} removed", job.getId(), job.getScheduleCron());
        } catch (Exception e) {
            throw new ArloRuntimeException(e);
        }
    }

    public boolean existsJob(SchedulerJobBo job) {
        try {
            return scheduler == null ? false : scheduler.checkExists(job.getJobKey());
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean existsTrigger(SchedulerJobBo job) {
        try {
            return scheduler == null ? false : scheduler.checkExists(job.getTriggerKey());
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isStarted() {
        try {
            return scheduler == null ? false : scheduler.isStarted();
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isShutdown() {
        try {
            return scheduler == null ? false : scheduler.isShutdown();
        } catch (SchedulerException e) {
            log.error("", e);
        }
        return false;
    }

    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void stop() throws Exception {
        if (scheduler == null || scheduler.isShutdown()) return;
        scheduler.shutdown();

        log.info("Quartz scheduler stopped.");
    }
}
