package io.github.artiship.arlo.scheduler.core.model;

import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.artiship.arlo.model.entity.SchedulerJob;
import io.github.artiship.arlo.model.enums.JobCycle;
import io.github.artiship.arlo.model.enums.JobPriority;
import io.github.artiship.arlo.model.enums.JobReleaseState;
import io.github.artiship.arlo.model.enums.JobType;
import io.github.artiship.arlo.model.exception.ArloRuntimeException;
import io.github.artiship.arlo.utils.CronUtils;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Splitter.on;
import static io.github.artiship.arlo.model.enums.JobType.MYSQL2HIVE;
import static java.lang.Long.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerKey.triggerKey;

@Slf4j
@Data
@Accessors(chain = true)
public class SchedulerJobBo implements Serializable {
    public static final String DATA_KEY = "data";
    public static final String DEFAULT_TRIGGER_GROUP = "arlo";
    public static final String DEFAULT_JOB_GROUP = "arlo";
    private static final long serialVersionUID = -2117547527224199953L;
    private Long id;
    private String jobName;
    private JobType jobType;
    private JobPriority jobPriority;
    private String creatorId;
    private String creatorName;
    private List<String> ownerIds;
    private List<String> ownerNames;
    private List<String> alertUsers;
    private List<Long> alertTypes;
    private String scheduleCron;
    private Long offsetMs;
    private Boolean isSelfDependent;
    private JobCycle jobCycle;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private Long executionTimeout;
    private List<String> workerGroups;
    private JobReleaseState jobReleaseState;
    private String description;
    private String jobConfiguration;
    private String ossPath;
    private List<SchedulerJobBo> dependencies = new ArrayList<>();

    public static SchedulerJobBo from(SchedulerJob schedulerJob) {
        if (null == schedulerJob)
            return null;

        return new SchedulerJobBo().setId(schedulerJob.getId())
                                   .setJobName(schedulerJob.getJobName())
                                   .setJobType(schedulerJob.getJobType() == null ? null
                                           : JobType.of(schedulerJob.getJobType()))
                                   .setJobPriority(schedulerJob.getJobPriority() == null ? null
                                           : JobPriority.of(schedulerJob.getJobPriority()))
                                   .setJobReleaseState(schedulerJob.getJobReleaseState() == null ? null
                                           : JobReleaseState.of(schedulerJob.getJobReleaseState()))
                                   .setOwnerNames(schedulerJob.getOwnerNames() == null ? null :
                                           on(",").splitToList(schedulerJob.getOwnerNames()))
                                   .setOwnerIds(schedulerJob.getOwnerIds() == null ? null :
                                           on(",").splitToList(schedulerJob.getOwnerIds()))
                                   .setCreatorId(schedulerJob.getCreatorId())
                                   .setCreatorName(schedulerJob.getCreatorName())
                                   .setAlertUsers(schedulerJob.getAlertUsers() == null ? null :
                                           on(",").splitToList(schedulerJob.getAlertUsers()))
                                   .setAlertTypes(schedulerJob.getAlertIds() == null ? null :
                                           on(",").splitToList(schedulerJob.getAlertIds())
                                                  .stream()
                                                  .map(i -> valueOf(i))
                                                  .collect(toList()))
                                   .setIsSelfDependent(schedulerJob.getIsSelfDependent() == null ? false
                                           : toBoolean(schedulerJob.getIsSelfDependent()))
                                   .setScheduleCron(schedulerJob.getScheduleCron())
                                   .setOffsetMs(schedulerJob.getOffsetMs())
                                   .setMaxRetryTimes(schedulerJob.getMaxRetryTimes())
                                   .setRetryInterval(schedulerJob.getRetryInterval())
                                   .setExecutionTimeout(schedulerJob.getExecutionTimeout())
                                   .setWorkerGroups(schedulerJob.getListOfWorkerGroups())
                                   .setDescription(schedulerJob.getDescription())
                                   .setJobConfiguration(schedulerJob.getJobConfiguration())
                                   .setOssPath(schedulerJob.getOssPath());
    }

    public String getOwnersString() {
        if (this.ownerNames == null) return "";

        return Joiner.on(",")
                     .skipNulls()
                     .join(this.ownerNames);
    }

    public SchedulerJobBo addDependency(SchedulerJobBo schedulerJobBo) {
        dependencies.add(schedulerJobBo);
        return this;
    }

    public SchedulerJobBo addDependencies(List<SchedulerJobBo> dependencies) {
        if (CollectionUtils.isEmpty(dependencies))
            return this;

        this.dependencies.addAll(dependencies);

        return this;
    }

    public SchedulerJob toSchedulerJobInfo() {
        return null;
    }

    public Set<Long> getParentIds() {
        if (this.dependencies == null)
            return null;

        return dependencies.stream()
                           .map(SchedulerJobBo::getId)
                           .collect(toSet());

    }

    public TriggerKey getTriggerKey() {
        return triggerKey(quartzKey(), DEFAULT_TRIGGER_GROUP);
    }

    public JobKey getJobKey() {
        return jobKey(quartzKey(), DEFAULT_TRIGGER_GROUP);
    }

    public String quartzKey() {
        return "arlo_" + this.id;
    }

    public JobDataMap getJobDataMap() {
        final JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(DATA_KEY, this);
        return jobDataMap;
    }

    public JobDetail getJobDetail(Class<? extends Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                         .withIdentity(getJobKey())
                         .usingJobData(getJobDataMap())
                         .storeDurably(true)
                         .build();
    }

    public CronTrigger getCronTrigger() {
        return TriggerBuilder.newTrigger()
                             .withIdentity(getTriggerKey())
                             .startNow()
                             .withSchedule(cronSchedule(this.scheduleCron).withMisfireHandlingInstructionIgnoreMisfires())
                             .build();
    }

    public String getSourceHost() {
        if (this.jobType != MYSQL2HIVE) return "";

        try {
            JsonParser parser = new JsonParser();
            JsonObject obj = parser.parse(this.jobConfiguration)
                                   .getAsJsonObject();
            return obj.get("host")
                      .getAsString();
        } catch (Exception e) {
            log.warn("Extract host from job configuration {} failed", this.jobConfiguration, e);
        }

        return "";
    }

    public Integer getHistorySize() {
        if (this.jobCycle == null) {
            if (this.scheduleCron == null) throw new ArloRuntimeException("Job " + id + " cron is null.");

            return CronUtils.jobCycle(this.scheduleCron)
                            .historySize();
        }

        return this.jobCycle.historySize();
    }

    public boolean isSelfDependent() {
        return this.isSelfDependent == null ? false : this.isSelfDependent;
    }

}