package io.github.artiship.arlo.model.entity;

import com.alibaba.fastjson.JSONObject;
import io.github.artiship.arlo.model.enums.JobReleaseState;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.time.LocalDateTime.now;
import static java.time.LocalDateTime.parse;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Collections.emptyList;
import static javax.management.timer.Timer.ONE_HOUR;

@Accessors(chain = true)
@Data
@Table("t_arlo_scheduler_job")
public class SchedulerJob {
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Id
    private Long id;
    private String jobName;
    private Integer jobType;
    private Integer jobPriority;
    private String creatorId;
    private String creatorName;
    private String ownerIds;
    private String ownerNames;
    private String alertUsers;
    private String config;
    private Integer jobCycle;

    private String alertIds;
    private String scheduleCron;
    private Long offsetMs;
    private Boolean isSelfDependent;
    private Integer dependencyType;
    private String dependencyRange;
    private String dependencyRule;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private Long executionTimeout;
    private String workerGroups;
    private Integer jobReleaseState;
    private String description;
    private String jobConfiguration;
    private Long businessLine;
    private String version;
    private String ossPath;
    private LocalDateTime startTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;


    private Boolean refreshSubJob;

    private Integer isPrivate;

    public static SchedulerJob from(Long jobId, JSONObject jobBasic, JSONObject jobSchedule, JSONObject jobVersion, int cron, String group, LocalDateTime createTime) {
        return new SchedulerJob()
                .setId(jobId)
                .setJobName(jobBasic.getString("jobName"))
                .setJobPriority(jobBasic.getInteger("jobPriority"))
                .setJobType(jobBasic.getInteger("jobType"))
                .setBusinessLine(jobBasic.getLong("businessLine"))
                .setCreatorId(jobBasic.getString("creators"))
                .setOwnerIds(jobBasic.getString("owners"))
                .setDescription(jobBasic.getString("desc"))
                .setStartTime(parse(jobSchedule.getString("startTime"), ofPattern(DATE_TIME_FORMAT)))
                .setScheduleCron(jobSchedule.getString("scheduleCron"))
                .setIsSelfDependent(jobSchedule.getBoolean("isSelfDependent") == null ? false : jobSchedule.getBoolean("isSelfDependent"))
                .setDependencyType(jobSchedule.getInteger("dependencyType"))
                .setDependencyRange(jobSchedule.getString("dependencyRange"))
                .setDependencyRule(jobSchedule.getString("dependencyRule"))
                .setRefreshSubJob(jobSchedule.getBoolean("refreshSubJob") == null ? false : jobSchedule.getBoolean("refreshSubJob"))
                .setIsPrivate(jobBasic.getInteger("isPrivate") == null ? 0 : jobBasic.getInteger("isPrivate"))
                .setMaxRetryTimes(jobSchedule.getInteger("retry"))
                .setJobReleaseState(jobBasic.getInteger("jobReleaseState"))
                .setRetryInterval(jobSchedule.getLong("retryInterval"))
                .setExecutionTimeout(jobSchedule.getLong("executionTimeout"))
                .setVersion(jobVersion.getString("version"))
                .setOssPath(jobVersion.getString("ossPath"))
                .setCreatorName(jobBasic.getString("creatorName"))
                .setConfig(jobSchedule.getString("config"))
                .setJobCycle(cron)
                .setOwnerNames(jobBasic.getString("ownerNames"))
                .setWorkerGroups(group)
                .setOffsetMs(jobSchedule.getLong("offsetMs") == null ? null : jobSchedule.getLong("offsetMs") * ONE_HOUR)
                .setCreateTime(createTime)
                .setUpdateTime(now());
    }

    public List<String> getListOfWorkerGroups() {
        if (this.workerGroups == null)
            return emptyList();

        return on(",").splitToList(this.getWorkerGroups());
    }

    public boolean jobIsDeleted() {
        return this.jobReleaseState == JobReleaseState.DELETED.getCode();
    }
}