package io.github.artiship.arlo.scheduler.core.model;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.protobuf.ProtocolStringList;
import io.github.artiship.arlo.model.ZkLostTask;
import io.github.artiship.arlo.model.entity.SchedulerTask;
import io.github.artiship.arlo.model.enums.JobPriority;
import io.github.artiship.arlo.model.enums.JobType;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.model.enums.TaskTriggerType;
import io.github.artiship.arlo.model.exception.ArloRuntimeException;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcTask;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Joiner.on;
import static com.google.common.collect.ComparisonChain.start;
import static com.google.common.collect.Ordering.natural;
import static io.github.artiship.arlo.model.enums.JobPriority.MEDIUM;
import static io.github.artiship.arlo.model.enums.TaskState.*;
import static io.github.artiship.arlo.model.enums.TaskTriggerType.MANUAL_RUN;
import static io.github.artiship.arlo.utils.CronUtils.*;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static io.github.artiship.arlo.utils.Dates.protoTimestamp;
import static java.time.Duration.between;
import static java.time.LocalDateTime.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;
import static org.apache.commons.lang3.BooleanUtils.toInteger;

@Data
@Accessors(chain = true)
public class SchedulerTaskBo implements Comparable<SchedulerTaskBo> {
    private Long id;
    private Long jobId;
    private String taskName;
    private TaskState taskState;
    private String ossPath;
    private List<String> workerGroups;
    private String workerHost;
    private Integer workerPort;
    private String creatorName;
    private String creatorEmail;
    private Integer maxRetryTimes;
    private Long retryInterval;
    private Integer retryTimes;
    private Long executionTimeout;
    private Long pid;
    private Long dagId;
    private List<String> applicationId;
    private TaskTriggerType taskTriggerType;
    private JobPriority jobPriority;
    private JobType jobType;
    private Boolean isSelfDependent;
    private String scheduleCron;
    private Long offsetMs;
    private String sourceHost;
    private LocalDateTime scheduleTime; //quartz fire time, includes missing fire time
    private LocalDateTime pendingTime; //time of submit to task scheduler
    private LocalDateTime waitingTime; //time of submit to task dispatcher
    private LocalDateTime dispatchedTime; //time of dispatched to worker
    private LocalDateTime startTime; //time of start running
    private LocalDateTime endTime;  //time of success/fail/killed
    private Long elapseTime;
    private Integer parallelism;
    private LocalDateTime calculationTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private Integer penalty = 0;
    private Optional<Boolean> isFirstOfJob = empty();
    private Set<Long> skipDependencies = new HashSet<>();
    private String taskDependenciesJson;

    public static SchedulerTaskBo from(SchedulerJobBo schedulerJobBo) {
        return new SchedulerTaskBo().setJobId(schedulerJobBo.getId())
                                    .setTaskName(schedulerJobBo.getJobName())
                                    .setCreatorName(schedulerJobBo.getOwnersString())
                                    .setOssPath(schedulerJobBo.getOssPath())
                                    .setWorkerGroups(schedulerJobBo.getWorkerGroups())
                                    .setMaxRetryTimes(schedulerJobBo.getMaxRetryTimes())
                                    .setRetryInterval(schedulerJobBo.getRetryInterval())
                                    .setExecutionTimeout(schedulerJobBo.getExecutionTimeout())
                                    .setScheduleTime(preScheduleTimeOfSomeTime(schedulerJobBo.getScheduleCron(), now()))
                                    .setJobPriority(schedulerJobBo.getJobPriority() != null ? schedulerJobBo.getJobPriority() : MEDIUM)
                                    .setSourceHost(schedulerJobBo.getSourceHost())
                                    .setJobType(schedulerJobBo.getJobType())
                                    .setIsSelfDependent(schedulerJobBo.getIsSelfDependent())
                                    .setScheduleCron(schedulerJobBo.getScheduleCron())
                                    .setOffsetMs(schedulerJobBo.getOffsetMs());
    }

    public static SchedulerTaskBo from(RpcTask task) {
        LocalDateTime startTime = task.getStartTime() == null ? null : localDateTime(task.getStartTime());
        LocalDateTime endTime = task.getEndTime() == null ? null : localDateTime(task.getEndTime());
        return new SchedulerTaskBo().setId(task.getId())
                                    .setJobId(task.getJobId())
                                    .setTaskState(TaskState.of(task.getState()))
                                    .setOssPath(task.getOssPath())
                                    .setWorkerHost(task.getWorkerHost())
                                    .setWorkerPort(task.getWorkerPort())
                                    .setJobType(JobType.of(task.getJobType()))
                                    .setMaxRetryTimes(task.getMaxRetryTimes())
                                    .setRetryInterval(task.getRetryInterval())
                                    .setRetryTimes(task.getRetryTimes())
                                    .setExecutionTimeout(task.getExecutionTimeout())
                                    .setPid(task.getPid())
                                    .setApplicationId(toApplicationList(task.getApplicationIdList()))
                                    .setScheduleTime(localDateTime(task.getScheduleTime()))
                                    .setCalculationTime(localDateTime(task.getCalculationTime()))
                                    .setStartTime(startTime.getYear() == 1970 ? null : startTime)
                                    .setEndTime(endTime.getYear() == 1970 ? null : endTime);
    }

    public static List<String> toApplicationList(ProtocolStringList list) {
        return list == null ? null : list.stream()
                                         .filter(i -> i != null && i.trim()
                                                                    .length() > 0)
                                         .map(i -> i.trim())
                                         .collect(Collectors.toList());
    }

    public static SchedulerTaskBo from(ZkLostTask zkLostTask) {
        return new SchedulerTaskBo().setId(zkLostTask.getId())
                                    .setTaskState(zkLostTask.getState())
                                    .setEndTime(zkLostTask.getEndTime());
    }

    public static SchedulerTaskBo from(SchedulerTask schedulerTask) {
        return new SchedulerTaskBo().setId(schedulerTask.getId())
                                    .setTaskName(schedulerTask.getTaskName())
                                    .setCreatorName(schedulerTask.getCreatorName())
                                    .setWorkerGroups(schedulerTask.getListOfWorkerGroups())
                                    .setWorkerHost(schedulerTask.getWorkerHost())
                                    .setWorkerPort(schedulerTask.getWorkerPort())
                                    .setTaskState(schedulerTask.getTaskState() == null ? null
                                            : of(schedulerTask.getTaskState()))
                                    .setElapseTime(schedulerTask.getElapseTime())
                                    .setParallelism(schedulerTask.getParallelism())
                                    .setPid(schedulerTask.getPid())
                                    .setDagId(schedulerTask.getDagId())
                                    .setApplicationId(schedulerTask.getApplicationId() == null ? null
                                            : Splitter.on(",")
                                                      .splitToList(schedulerTask.getApplicationId()))
                                    .setJobId(schedulerTask.getJobId())
                                    .setTaskTriggerType(schedulerTask.getTaskTriggerType() == null ? null
                                            : TaskTriggerType.of(schedulerTask.getTaskTriggerType()))
                                    .setIsSelfDependent(schedulerTask.getIsSelfDependent() == null ? null
                                            : toBoolean(schedulerTask.getIsSelfDependent()))
                                    .setScheduleCron(schedulerTask.getScheduleCron())
                                    .setOffsetMs(schedulerTask.getOffsetMs())
                                    .setSourceHost(schedulerTask.getSourceHost())
                                    .setTaskDependenciesJson(schedulerTask.getDependenciesJson())
                                    .setCreatorEmail(schedulerTask.getCreatorEmail())
                                    .setCreatorName(schedulerTask.getCreatorName())
                                    .setExecutionTimeout(schedulerTask.getExecutionTimeout())
                                    .setRetryInterval(schedulerTask.getRetryInterval())
                                    .setMaxRetryTimes(schedulerTask.getMaxRetryTimes())
                                    .setRetryTimes(schedulerTask.getRetryTimes())
                                    .setJobType(schedulerTask.getJobType() == null ? null
                                            : JobType.of(schedulerTask.getJobType()))
                                    .setJobPriority(schedulerTask.getJobPriority() == null ? null
                                            : JobPriority.of(schedulerTask.getJobPriority()))
                                    .setOssPath(schedulerTask.getOssPath())
                                    .setScheduleTime(schedulerTask.getScheduleTime())
                                    .setPendingTime(schedulerTask.getPendingTime())
                                    .setWaitingTime(schedulerTask.getWaitingTime())
                                    .setDispatchedTime(schedulerTask.getDispatchedTime())
                                    .setStartTime(schedulerTask.getStartTime())
                                    .setEndTime(schedulerTask.getEndTime())
                                    .setCreateTime(schedulerTask.getCreateTime())
                                    .setUpdateTime(schedulerTask.getUpdateTime());
    }

    public static SchedulerTaskBo from(String jsonStr) {
        return new Gson().fromJson(jsonStr, SchedulerTaskBo.class);
    }

    public static SchedulerTaskBo from(byte[] jsonBytes) {
        return from(new String(jsonBytes, UTF_8));
    }

    public SchedulerTask toSchedulerTask() {
        return new SchedulerTask().setId(id)
                                  .setJobId(this.getJobId())
                                  .setTaskName(this.getTaskName())
                                  .setTaskState(this.getTaskStateCode())
                                  .setOssPath(this.getOssPath())
                                  .setWorkerGroups(this.workerGroupsToString())
                                  .setWorkerHost(this.getWorkerHost())
                                  .setWorkerPort(this.getWorkerPort())
                                  .setRetryInterval(this.getRetryInterval())
                                  .setCreatorName(this.getCreatorName())
                                  .setCreatorEmail(this.getCreatorEmail())
                                  .setMaxRetryTimes(this.getMaxRetryTimes())
                                  .setExecutionTimeout(this.getExecutionTimeout())
                                  .setPid(this.getPid())
                                  .setDagId(this.getDagId())
                                  .setApplicationId(this.applicationIdsToString())
                                  .setRetryTimes(this.getRetryTimes())
                                  .setTaskTriggerType(this.getTaskTriggerTypeCode())
                                  .setJobPriority(this.jobPriority == null ? null : this.jobPriority.getPriority())
                                  .setJobType(this.getJobTypeCode())
                                  .setIsSelfDependent(getIsSelfDependentValue())
                                  .setScheduleCron(this.getScheduleCron())
                                  .setOffsetMs(this.getOffsetMs())
                                  .setParallelism(this.getParallelism())
                                  .setSourceHost(this.getSourceHost())
                                  .setDependenciesJson(this.taskDependenciesJson)
                                  .setScheduleTime(this.getScheduleTime())
                                  .setPendingTime(this.getPendingTime())
                                  .setWaitingTime(this.getWaitingTime())
                                  .setDispatchedTime(this.getDispatchedTime())
                                  .setStartTime(this.getStartTime())
                                  .setEndTime(this.getEndTime())
                                  .setElapseTime(this.getElapseTime())
                                  .setCreateTime(this.getCreateTime())
                                  .setUpdateTime(this.getUpdateTime());
    }

    public RpcTask toRpcTask() {
        RpcTask.Builder builder = RpcTask.newBuilder();
        if (this.workerHost != null) builder.setWorkerHost(this.workerHost);
        if (this.executionTimeout != null) builder.setExecutionTimeout(this.executionTimeout);
        if (this.id != null) builder.setId(this.id);
        if (this.id != null) builder.setJobId(this.jobId);
        if (this.retryInterval != null) builder.setRetryInterval(this.retryInterval);
        if (this.scheduleTime != null) builder.setScheduleTime(protoTimestamp(scheduleTime));
        if (this.calculationTime != null) {
            builder.setCalculationTime(protoTimestamp(this.calculationTime));
        } else if (this.scheduleCron != null && this.scheduleTime != null) {
            builder.setCalculationTime(protoTimestamp(computeCalDate()));
        } else if (this.scheduleCron == null) { //job hasn't been scheduled but run manually
            builder.setCalculationTime(protoTimestamp(now()));
        }
        if (this.startTime != null) builder.setStartTime(protoTimestamp(this.startTime));
        if (this.applicationId != null) {
            this.applicationId.forEach(appId -> builder.addApplicationId(appId));
        }
        if (this.endTime != null) builder.setEndTime(protoTimestamp(this.endTime));
        if (this.retryTimes != null) builder.setRetryTimes(retryTimes);
        if (this.maxRetryTimes != null) builder.setMaxRetryTimes(maxRetryTimes);
        if (this.taskState != null) builder.setState(taskState.getCode());
        if (this.workerPort != null) builder.setWorkerPort(workerPort);
        if (this.jobType != null) builder.setJobType(jobType.getCode());
        if (this.ossPath != null) builder.setOssPath(ossPath);
        return builder.build();
    }

    public SchedulerTaskBo toRenewTask() {
        return new SchedulerTaskBo().setJobId(this.jobId)
                                    .setTaskName(this.taskName)
                                    .setOssPath(this.ossPath)
                                    .setWorkerGroups(this.getWorkerGroups())
                                    .setCreatorEmail(this.creatorEmail)
                                    .setCreatorName(this.creatorName)
                                    .setRetryTimes((this.retryTimes == null ? 0 : this.retryTimes) + 1)
                                    .setRetryInterval(this.retryInterval)
                                    .setMaxRetryTimes(this.maxRetryTimes)
                                    .setExecutionTimeout(this.executionTimeout)
                                    .setJobPriority(this.jobPriority)
                                    .setJobType(this.jobType)
                                    .setDagId(this.dagId)
                                    .setIsSelfDependent(this.isSelfDependent)
                                    .setScheduleCron(this.scheduleCron)
                                    .setOffsetMs(this.offsetMs)
                                    .setParallelism(this.parallelism)
                                    .setSourceHost(this.sourceHost)
                                    .setCalculationTime(this.calculationTime)
                                    .setScheduleTime(this.scheduleTime)
                                    .setTaskTriggerType(this.taskTriggerType);
    }

    public SchedulerTaskBo copy() {
        return new SchedulerTaskBo().setId(this.id)
                                    .setJobId(this.jobId)
                                    .setTaskName(this.taskName)
                                    .setTaskState(this.taskState)
                                    .setOssPath(this.ossPath)
                                    .setWorkerGroups(this.workerGroups)
                                    .setWorkerHost(this.workerHost)
                                    .setWorkerPort(this.workerPort)
                                    .setCreatorName(this.creatorName)
                                    .setCreatorEmail(this.creatorEmail)
                                    .setMaxRetryTimes(this.maxRetryTimes)
                                    .setRetryInterval(this.retryInterval)
                                    .setRetryTimes(this.retryTimes)
                                    .setExecutionTimeout(this.executionTimeout)
                                    .setPid(this.pid)
                                    .setDagId(this.dagId)
                                    .setApplicationId(this.applicationId)
                                    .setTaskTriggerType(this.taskTriggerType)
                                    .setJobPriority(this.jobPriority)
                                    .setJobType(this.jobType)
                                    .setIsSelfDependent(this.isSelfDependent)
                                    .setScheduleCron(this.scheduleCron)
                                    .setOffsetMs(this.offsetMs)
                                    .setSourceHost(this.sourceHost)
                                    .setScheduleTime(this.scheduleTime)
                                    .setPendingTime(this.pendingTime)
                                    .setWaitingTime(this.waitingTime)
                                    .setDispatchedTime(this.dispatchedTime)
                                    .setStartTime(this.startTime)
                                    .setEndTime(this.endTime)
                                    .setElapseTime(this.elapseTime)
                                    .setParallelism(this.parallelism)
                                    .setCalculationTime(this.calculationTime)
                                    .setCreateTime(this.createTime)
                                    .setUpdateTime(this.updateTime)
                                    .setPenalty(this.penalty)
                                    .setIsFirstOfJob(this.isFirstOfJob)
                                    .setSkipDependencies(this.skipDependencies)
                                    .setTaskDependenciesJson(this.taskDependenciesJson);
    }

    private String workerGroupsToString() {
        return this.getWorkerGroups() == null ? null : on(",").join(this.getWorkerGroups());
    }

    public Integer getJobTypeCode() {
        return this.jobType == null ? null : this.jobType.getCode();
    }

    public Integer getIsSelfDependentValue() {
        return this.isSelfDependent == null ? null : toInteger(this.isSelfDependent);
    }

    public String applicationIdsToString() {
        return this.getApplicationId() == null ? null : on(",").join(this.getApplicationId());
    }

    public Integer getTaskStateCode() {
        return this.taskState == null ? null : taskState.getCode();
    }

    public Integer getTaskTriggerTypeCode() {
        return this.taskTriggerType == null ? null : taskTriggerType.getCode();
    }

    public Boolean isTriggerManually() {
        return this.taskTriggerType == MANUAL_RUN;
    }

    private LocalDateTime computeCalDate() {
        if (this.offsetMs == null || this.offsetMs == 0) {
            return previousScheduleTimeOf(this.scheduleCron, this.scheduleTime);
        }

        return this.scheduleTime.plus(this.offsetMs, MILLIS);
    }

    public boolean isTerminated() {
        if (taskState == FAIL || taskState == SUCCESS || taskState == KILLED) {
            return true;
        }
        return false;
    }

    public boolean retryable() {
        if (this.maxRetryTimes == null || this.maxRetryTimes == 0)
            return false;

        if ((this.retryTimes == null ? 0 : this.retryTimes) >= maxRetryTimes)
            return false;

        return true;
    }

    public boolean isSelfDependent() {
        return this.isSelfDependent == null ? false : this.isSelfDependent;
    }

    public SchedulerTaskBo penalty() {
        this.penalty++;
        return this;
    }

    public Long getElapseTime() {
        if (this.elapseTime != null)
            return this.elapseTime;

        if (this.startTime != null && this.endTime != null)
            return between(this.startTime, this.endTime).toMillis();

        return null;
    }

    public boolean isFirstOfJob() {
        if (!this.isFirstOfJob.isPresent())
            return false;

        return isFirstOfJob.get();
    }

    public int getHistorySize() {
        if (this.scheduleCron == null)
            throw new ArloRuntimeException("Task " + id + " cron is null.");

        return jobCycle(this.scheduleCron).historySize();
    }

    public void addSkipDependency(Long parentJobId) {
        this.skipDependencies.add(parentJobId);
    }

    public void addSkipDecencies(Set<Long> parentJobIds) {
        this.skipDependencies.addAll(parentJobIds);
    }

    public boolean shouldSkipDependency(Long parentJobId) {
        return this.skipDependencies.contains(parentJobId);
    }

    public int parallelism() {
        return this.parallelism == null ? 0 : this.parallelism;
    }

    @Override
    public int compareTo(SchedulerTaskBo that) {
        return start().compare(this.jobPriority, that.jobPriority, natural().reverse())
                      .compare(this.penalty, that.penalty)
                      .compare(this.scheduleTime, that.scheduleTime, natural().nullsFirst())
                      .result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchedulerTaskBo taskBo = (SchedulerTaskBo) o;
        return id.equals(taskBo.id) &&
                taskBo.scheduleTime.truncatedTo(SECONDS)
                                   .equals(scheduleTime == null ? null : scheduleTime.truncatedTo(SECONDS));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, scheduleTime);
    }

    public void incrRetryTimes() {
        if (this.retryTimes == null) this.retryTimes = 1;
        this.retryTimes++;
    }
}