package io.github.artiship.arlo.scheduler.manager;

import com.google.common.collect.ImmutableSet;
import io.github.artiship.arlo.model.bo.TaskDependency;
import io.github.artiship.arlo.model.enums.JobType;
import io.github.artiship.arlo.model.vo.JobRelation;
import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.manager.collections.LimitedSortedByValueMap;
import io.github.artiship.arlo.scheduler.manager.collections.LimitedSortedSet;
import io.github.artiship.arlo.scheduler.manager.dependency.DependencyBuilder;
import io.github.artiship.arlo.scheduler.model.TaskFailedRecord;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.Ordering.natural;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.github.artiship.arlo.model.bo.TaskDependency.toTaskDependenciesJson;
import static io.github.artiship.arlo.scheduler.model.TaskFailedRecord.of;
import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.CronUtils.jobCycle;
import static io.github.artiship.arlo.utils.Dates.localDateTimeToStr;
import static io.github.artiship.arlo.utils.QuartzUtils.preScheduleTime;
import static java.lang.Math.min;
import static java.time.LocalDateTime.now;
import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.springframework.util.CollectionUtils.isEmpty;

@Slf4j
@Component
public class JobStateStore implements Service {
    private final Map<Long, Map<String, TaskSuccessRecord>> jobSuccessHistory = new ConcurrentHashMap<>();
    private final Map<Long, NavigableSet<TaskFailedRecord>> jobFailedWorkers = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> jobRunningTasks = new ConcurrentHashMap<>();
    private final Map<String, Set<Long>> sourceHostRunningTasks = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> jobDependencies = new ConcurrentHashMap<>();

    @Value("${services.task-scheduler.job.max-concurrency:10}")
    private int jobMaxConcurrency;
    @Value("${services.task-scheduler.job.failed-workers:3}")
    private int jobFailedWorkerSize;

    @Autowired
    private SchedulerDao schedulerDao;

    @Autowired
    private DagScheduler dagScheduler;

    @Override
    public void start() {
        jobDependencies.clear();
        jobFailedWorkers.clear();
        jobRunningTasks.clear();

        log.info("Reload job dependencies from db into job state store.");
        schedulerDao.getJobRelations()
                    .forEach(this::addJobDependency);
    }

    @Override
    public void stop() {

    }

    public void addJobDependency(JobRelation jobRelation) {
        requireNonNull(jobRelation, "Job relation can not be null");

        Set<Long> parents = jobDependencies.get(jobRelation.getJobId());
        if (parents == null) {
            parents = synchronizedSet(new LinkedHashSet<>());
            Set<Long> pre = jobDependencies.putIfAbsent(jobRelation.getJobId(), parents);
            if (pre != null) parents = pre;
        }

        parents.add(jobRelation.getParentJobId());

        log.info("Job_{} parent {} add to dependency store", jobRelation.getJobId(), jobRelation.getParentJobId());
    }


    public void updateJobDependencies(Long jobId, Set<Long> dependencies) {
        log.info("Job_{} update dependencies {}", jobId, dependencies);
        jobDependencies.put(jobId, dependencies);
    }

    public void removeJob(Long jobId) {
        Set<Long> removed = jobDependencies.remove(jobId);
        log.info("Job_{} all dependencies {} removed from dependency store", jobId, removed);
        jobSuccessHistory.remove(jobId);
        jobFailedWorkers.remove(jobId);
        jobRunningTasks.remove(jobId);
    }

    public void removeJobDependency(JobRelation relation) {
        this.removeJobDependency(relation.getJobId(), relation.getParentJobId());
    }

    public void removeJobDependency(Long jobId, Long parentJobId) {
        requireNonNull(jobId, "Job id can not be null");
        requireNonNull(parentJobId, "Parent job id can not be null");

        Set<Long> dependencies = jobDependencies.get(jobId);
        if (isEmpty(dependencies))
            return;

        dependencies.remove(parentJobId);

        log.info("Job_{} parent {} removed from dependency store", jobId, parentJobId);
    }

    public void taskSuccess(final SchedulerTaskBo task) {
        addTaskSuccessRecord(task);
        removeJobRunningTask(task);

        if (task.getDagId() != null) {
            this.dagScheduler.advanceDag(task);
        }
    }

    public void taskFailed(final SchedulerTaskBo task) {
        addFailedHost(task.getJobId(), task.getWorkerHost());
        removeJobRunningTask(task);
        removeTaskSuccessRecord(task);
    }

    public void failoverTask(final SchedulerTaskBo task) {
        removeJobRunningTask(task);
    }

    public TaskSuccessRecord addTaskSuccessRecord(final SchedulerTaskBo task) {
        TaskSuccessRecord record = TaskSuccessRecord.of(task.getId(), task.getScheduleCron(), task.getScheduleTime(), task.getElapseTime());
        Map<String, TaskSuccessRecord> history = this.getOrLoadSuccessHistory(task.getJobId());
        history.put(record.getCalTimeRangeStr(), record);

        log.info("Task_{}_{} ADDED to success store: size={}, schedule_time={}.",
                task.getJobId(), task.getId(), history.size(), task.getScheduleTime());

        return record;
    }

    public void removeTaskSuccessRecord(final SchedulerTaskBo task) {
        Map<String, TaskSuccessRecord> history = this.getOrLoadSuccessHistory(task.getJobId());
        TaskSuccessRecord removed = history.remove(calTimeRangeStr(task.getScheduleTime(), task.getScheduleCron()));

        if (removed != null) {
            log.info("Task_{}_{} REMOVED from success store: size:{}, schedule_time={}.",
                    task.getJobId(), task.getId(), history.size(), task.getScheduleTime());
        }
    }

    public Boolean hasNoDependencies(Long jobId) {
        return isEmpty(jobDependencies.get(jobId));
    }

    public Set<Long> getDependencies(Long id) {
        Set<Long> dependencies = this.jobDependencies.get(id);
        if (dependencies == null)
            return emptySet();

        return unmodifiableSet(dependencies);
    }

    public void addFailedHost(Long jobId, String host) {
        getOrLoadFailedHosts(jobId).add(of(now(), host));

        log.info("Job_{} ADDED to failed host store: worker={}", jobId, host);
    }

    public List<String> getFailedHosts(Long jobId) {
        Set<TaskFailedRecord> failedHosts = getOrLoadFailedHosts(jobId);

        return failedHosts.stream()
                          .filter(i -> i.latestOneDay())
                          .map(i -> i.getWorkerHost())
                          .collect(toList());
    }

    public boolean exceedConcurrencyLimit(Long jobId, Integer parallelism) {
        int limit = parallelism == 0 ? jobMaxConcurrency : min(parallelism, jobMaxConcurrency);
        return this.getOrLoadJobRunningTasks(jobId)
                   .size() >= limit;
    }

    public boolean extractLimit(String sourceHost, final int extractLimit) {
        if (StringUtils.isEmpty(sourceHost)) return false;
        return this.getOrLoadSameSourceRunningTasks(sourceHost)
                   .size() >= extractLimit;
    }

    public void addJobRunningTask(final SchedulerTaskBo task) {
        Set<Long> runningTasks = getOrLoadJobRunningTasks(task.getJobId());
        runningTasks.add(task.getId());
        log.info("Task_{}_{} ADDED to concurrency limiter: cur={}, max={}",
                task.getJobId(), task.getId(), runningTasks.size(), this.jobMaxConcurrency);

        if (task.getJobType() == JobType.MYSQL2HIVE && !StringUtils.isEmpty(task.getSourceHost())) {
            Set<Long> sourceRunningTasks = this.getOrLoadSameSourceRunningTasks(task.getSourceHost());
            sourceRunningTasks.add(task.getId());
            log.info("Task_{}_{} ADDED to extract limiter: cur={}, max={}",
                    task.getJobId(), task.getId(), runningTasks.size(), this.jobMaxConcurrency);
        }
    }

    public void removeJobRunningTask(SchedulerTaskBo task) {
        Set<Long> runningTasks = getOrLoadJobRunningTasks(task.getJobId());
        if (runningTasks.remove(task.getId())) {
            log.info("Task_{}_{} REMOVED from concurrency limiter: cur={}, max={}",
                    task.getJobId(), task.getId(), runningTasks.size(), this.jobMaxConcurrency);
        }

        if (task == null || task.getJobType() != JobType.MYSQL2HIVE || StringUtils.isEmpty(task.getSourceHost()))
            return;

        Set<Long> sourceRunningTasks = this.getOrLoadSameSourceRunningTasks(task.getSourceHost());
        if (sourceRunningTasks.remove(task.getId())) {
            log.info("Task_{}_{} REMOVED from extract limiter: cur={}, max={}",
                    task.getJobId(), task.getId(), runningTasks.size(), 1);
        }
    }

    private Set<Long> getOrLoadJobRunningTasks(Long jobId) {
        Set<Long> runningTasks = jobRunningTasks.get(jobId);
        if (runningTasks == null) {
            runningTasks = newConcurrentHashSet();
            runningTasks.addAll(schedulerDao.getJobConcurrentTasks(jobId));
            Set<Long> previous = jobRunningTasks.putIfAbsent(jobId, runningTasks);
            if (previous != null)
                return previous;
        }
        return runningTasks;
    }

    private Set<Long> getOrLoadSameSourceRunningTasks(String sourceHost) {
        Set<Long> runningTasks = sourceHostRunningTasks.get(sourceHost);
        if (runningTasks == null) {
            runningTasks = newConcurrentHashSet();
            runningTasks.addAll(schedulerDao.getSourceHostConcurrentTasks(sourceHost));
            Set<Long> previous = sourceHostRunningTasks.putIfAbsent(sourceHost, runningTasks);
            if (previous != null)
                return previous;
        }
        return runningTasks;
    }

    public Set<Long> getJobRunningTasks(Long jobId) {
        return ImmutableSet.copyOf(this.jobRunningTasks.get(jobId));
    }

    public Set<TaskSuccessRecord> getJobSuccessHistory(Long jobId) {
        return ImmutableSet.copyOf(getOrLoadSuccessHistory(jobId).values());
    }

    private Map<String, TaskSuccessRecord> getOrLoadSuccessHistory(Long jobId) {
        requireNonNull(jobId, "Job id is null");

        SchedulerJobBo job = schedulerDao.getJob(jobId);
        Integer historySize = jobCycle(job.getScheduleCron()).historySize();

        return getOrLoadSuccessHistory(jobId, historySize);
    }

    private Map<String, TaskSuccessRecord> getOrLoadSuccessHistory(Long jobId, Integer historySize) {
        requireNonNull(jobId, "Job id is null");

        Map<String, TaskSuccessRecord> successRecords = jobSuccessHistory.get(jobId);
        if (successRecords == null) {
            successRecords = synchronizedSortedMap(new LimitedSortedByValueMap<String, TaskSuccessRecord>(natural(), historySize));
            for (TaskSuccessRecord record : schedulerDao.getLatestSuccessTasks(jobId, historySize)) {
                successRecords.put(record.getCalTimeRangeStr(), record);
            }

            Map<String, TaskSuccessRecord> previous = jobSuccessHistory.putIfAbsent(jobId, successRecords);
            if (previous != null)
                return previous;
        }

        return successRecords;
    }

    private Set<TaskFailedRecord> getOrLoadFailedHosts(Long jobId) {
        NavigableSet<TaskFailedRecord> failedHosts = jobFailedWorkers.get(jobId);
        if (failedHosts == null) {
            failedHosts = synchronizedNavigableSet(new LimitedSortedSet<>(jobFailedWorkerSize));
            failedHosts.addAll(schedulerDao.getLatestFailedHosts(jobId, jobFailedWorkerSize)
                                           .stream()
                                           .map(host -> TaskFailedRecord.of(now(), host))
                                           .collect(toSet()));

            NavigableSet<TaskFailedRecord> taskFailedRecords = jobFailedWorkers.putIfAbsent(jobId, failedHosts);
            if (taskFailedRecords != null)
                return taskFailedRecords;
        }
        return failedHosts;
    }

    public boolean isTaskTheFirstOfJob(final SchedulerTaskBo task) {
        boolean isFirstOfJob = task.getIsFirstOfJob()
                                   .orElseGet(() -> schedulerDao.isTaskTheFirstInstanceOfJob(task.getJobId(), task.getScheduleTime()));

        task.setIsFirstOfJob(Optional.of(isFirstOfJob));

        return isFirstOfJob;
    }

    private Set<TaskDependency> buildTaskDependencies(SchedulerTaskBo task) {
        Set<TaskDependency> dependencies = new HashSet<>();
        if (schedulerDao.isJobSelfDepend(task.getJobId())) {
            if (!isTaskTheFirstOfJob(task)) {
                LocalDateTime preScheduleTime = preScheduleTime(task.getScheduleCron(), task.getScheduleTime());
                String preCalTimeRange = calTimeRangeStr(preScheduleTime, task.getScheduleCron());

                dependencies.add(new TaskDependency(task.getJobId(),
                        localDateTimeToStr(task.getScheduleTime()), preCalTimeRange));
            }
        }

        Set<Long> parentJobIds = jobDependencies.get(task.getJobId());

        if (parentJobIds == null || parentJobIds.isEmpty()) {
            return dependencies;
        }

        for (Long parentJobId : parentJobIds) {
            if (parentJobId == null) continue;
            if (parentJobId == -1) continue;
            if (parentJobId == task.getJobId()) continue;
            if (task.shouldSkipDependency(parentJobId)) continue;

            SchedulerJobBo parentJob = schedulerDao.getJob(parentJobId);

            DependencyBuilder builder = DependencyBuilder.builder()
                                                         .childCronExpression(task.getScheduleCron())
                                                         .childScheduleTime(task.getScheduleTime())
                                                         .parentCronExpression(parentJob.getScheduleCron())
                                                         .isParentSelfDepend(parentJob.getIsSelfDependent())
                                                         .build();

            builder.parentScheduleTimes()
                   .forEach(scheduleTime ->
                           dependencies.add(new TaskDependency(parentJobId,
                                   localDateTimeToStr(scheduleTime), calTimeRangeStr(scheduleTime, parentJob.getScheduleCron()))));
        }

        return dependencies;
    }

    public boolean isDependencyReady(SchedulerTaskBo child) {
        Set<TaskDependency> taskDependencies = buildTaskDependencies(child);

        for (TaskDependency dependency : taskDependencies) {
            Map<String, TaskSuccessRecord> history = getOrLoadSuccessHistory(dependency.getJobId());
            TaskSuccessRecord record = history.get(dependency.getCalTimeRange());

            if (record != null) {
                dependency.setTaskCron(record.getTaskCron())
                          .setScheduleTime(localDateTimeToStr(record.getScheduleTime()))
                          .setTaskId(record.getTaskId())
                          .setReady(true);
            }
        }

        String taskDependenciesJson = toTaskDependenciesJson(taskDependencies);

        if (!taskDependenciesJson.equals(child.getTaskDependenciesJson())) {
            child.setTaskDependenciesJson(taskDependenciesJson);
            schedulerDao.updateTaskDependencies(child.getId(), taskDependenciesJson);
            log.info("Task_{}_{} ACQUIRED dependencies: {}", child.getJobId(), child.getId(), taskDependenciesJson);
        }

        if (taskDependencies.isEmpty()) return true;

        if (taskDependencies.stream()
                            .filter(t -> t.isReady())
                            .count() == taskDependencies.size()) return true;

        return false;
    }

    public boolean isPreReady(final SchedulerTaskBo task) {
        if (isTaskTheFirstOfJob(task)) {
            return true;
        }

        return isReady(task.getJobId(), preCalTimeRange(task.getScheduleCron(), task.getScheduleTime()));
    }

    public boolean isReady(final Long jobId, final String calTimeRange) {
        return getOrLoadSuccessHistory(jobId).get(calTimeRange) != null ? true : false;
    }

    private String preCalTimeRange(final String cron, final LocalDateTime scheduleTime) {
        return calTimeRangeStr(preScheduleTime(cron, scheduleTime), cron);
    }

    public void taskKilled(SchedulerTaskBo task) {
        this.removeJobRunningTask(task);
    }

    public int runningTaskCounts() {
        return this.jobRunningTasks.values()
                                   .stream()
                                   .map(set -> set.size())
                                   .reduce(0, Integer::sum);
    }
}
