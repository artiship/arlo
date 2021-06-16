package io.github.artiship.arlo.scheduler.manager;

import com.google.common.collect.ImmutableSet;
import io.github.artiship.arlo.db.repository.SchedulerDagRepository;
import io.github.artiship.arlo.db.repository.SchedulerJobRelationRepository;
import io.github.artiship.arlo.db.repository.SchedulerJobRepository;
import io.github.artiship.arlo.db.repository.SchedulerNodeRepository;
import io.github.artiship.arlo.db.repository.SchedulerTaskRepository;
import io.github.artiship.arlo.model.bo.SchedulerDagBo;
import io.github.artiship.arlo.model.entity.SchedulerDag;
import io.github.artiship.arlo.model.entity.SchedulerJob;
import io.github.artiship.arlo.model.entity.SchedulerJobRelation;
import io.github.artiship.arlo.model.entity.SchedulerNode;
import io.github.artiship.arlo.model.entity.SchedulerTask;
import io.github.artiship.arlo.model.enums.DagState;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.model.vo.JobRelation;
import io.github.artiship.arlo.scheduler.core.exception.DagNotFoundException;
import io.github.artiship.arlo.scheduler.core.exception.JobNotFoundException;
import io.github.artiship.arlo.scheduler.core.exception.TaskNotFoundException;
import io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerNodeBo;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Joiner.on;
import static io.github.artiship.arlo.model.enums.DagState.finishedStateCodes;
import static io.github.artiship.arlo.model.enums.DagType.DYNAMIC;
import static io.github.artiship.arlo.model.enums.DagType.STATIC;
import static io.github.artiship.arlo.model.enums.JobReleaseState.OFFLINE;
import static io.github.artiship.arlo.model.enums.JobReleaseState.ONLINE;
import static io.github.artiship.arlo.model.enums.NodeState.DEAD;
import static io.github.artiship.arlo.model.enums.TaskState.*;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerJobBo.from;
import static io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo.from;
import static io.github.artiship.arlo.utils.Dates.localDateTimeToStr;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.util.Lists.emptyList;

@Slf4j
@Component
public class SchedulerDao {

    private static final String SQL_LATEST_SUCCESS_TASKS = "SELECT id, " +
            " job_id, " +
            " schedule_cron, " +
            " schedule_time, " +
            " (end_time - start_time) AS execution_cost " +
            "FROM ( " +
            " SELECT id, " +
            "  job_id, " +
            "  schedule_cron, " +
            "  schedule_time, " +
            "  task_state, " +
            "  end_time, " +
            "  start_time " +
            " FROM ( " +
            "  SELECT id, " +
            "   job_id, " +
            "   schedule_cron, " +
            "   schedule_time, " +
            "   task_state, " +
            "   end_time, " +
            "   start_time " +
            "  FROM t_arlo_scheduler_task " +
            "  WHERE job_id = ? " +
            "  ORDER BY update_time DESC limit 100000 " +
            "  ) AS t " +
            " GROUP BY t.schedule_time " +
            " ) x " +
            "WHERE x.task_state = " + SUCCESS.getCode() + " " +
            "ORDER BY schedule_time DESC limit ?";
    @Autowired
    private SchedulerJobRepository schedulerJobRepository;
    @Autowired
    private SchedulerJobRelationRepository schedulerJobRelationRepository;
    @Autowired
    private SchedulerTaskRepository schedulerTaskRepository;
    @Autowired
    private SchedulerNodeRepository schedulerNodeRepository;
    @Autowired
    private SchedulerDagRepository schedulerDagRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public SchedulerJobBo getJobAndDependencies(Long jobId) {
        SchedulerJobBo schedulerJobBo = this.getJob(jobId);
        List<SchedulerJobBo> dependencies = this.getJobDependencies(schedulerJobBo.getId());
        schedulerJobBo.addDependencies(dependencies);

        return schedulerJobBo;
    }

    public List<SchedulerJobBo> getJobDependencies(Long jobId) {
        List<SchedulerJobRelation> parents = schedulerJobRelationRepository.findParentsByJobId(jobId);

        if (parents != null && !parents.isEmpty()) {
            return this.getJobs(parents.stream()
                                       .map(i -> i.getParentJobId())
                                       .collect(toList()));
        }

        return null;
    }

    public List<SchedulerJobBo> getJobs(List<Long> jobIds) {
        List<SchedulerJob> parentJobs =
                schedulerJobRepository.findAllByIdsAndJobReleaseState(jobIds, ONLINE.getCode());

        if (parentJobs != null && !parentJobs.isEmpty()) {
            return parentJobs.stream()
                             .map(i -> from(i))
                             .collect(toList());
        }

        return null;
    }

    public SchedulerJobBo getJob(Long jobId) {
        return from(schedulerJobRepository.findById(jobId)
                                          .orElseThrow(() -> new JobNotFoundException(jobId)));
    }

    public void updateDagState(Long dagId, DagState state) {
        SchedulerDag schedulerDag = this.schedulerDagRepository
                .findById(dagId)
                .orElseThrow(() -> new DagNotFoundException(dagId));

        schedulerDagRepository.save(schedulerDag.setDagState(state.getCode()));
    }

    public SchedulerTaskBo saveTask(SchedulerTaskBo schedulerTaskBo) {
        SchedulerTask schedulerTask = schedulerTaskBo.toSchedulerTask();
        schedulerTask.setUpdateTime(now());
        final Long taskId = schedulerTask.getId();
        if (taskId == null) {
            schedulerTask.setCreateTime(schedulerTask.getUpdateTime());
            return from(schedulerTaskRepository.save(schedulerTask));
        }

        SchedulerTask schedulerTaskFromDb = schedulerTaskRepository.findById(taskId)
                                                                   .orElseThrow(() -> new TaskNotFoundException(taskId));

        return from(schedulerTaskRepository.save(schedulerTaskFromDb.updateIgnoreNull(schedulerTask)));
    }

    public SchedulerDagBo saveDag(SchedulerDagBo schedulerDagBo) {
        SchedulerDag schedulerDag = schedulerDagBo.toSchedulerDag();
        schedulerDag.setUpdateTime(now());

        final Long dagId = schedulerDag.getId();
        if (dagId == null) {
            schedulerDag.setCreateTime(schedulerDag.getUpdateTime());
            return SchedulerDagBo.from(schedulerDagRepository.save(schedulerDag));
        }

        SchedulerDag schedulerDagFromDb = schedulerDagRepository.findById(dagId)
                                                                .orElseThrow(() -> new DagNotFoundException(dagId));

        return SchedulerDagBo.from(schedulerDagRepository.save(schedulerDagFromDb.updateIgnoreNull(schedulerDag)));
    }

    public List<SchedulerTaskBo> getRunningTasksByWorker(String ip) {
        List<SchedulerTaskBo> tasks = emptyList();
        List<SchedulerTask> runningTasks = schedulerTaskRepository
                .findAllTaskByWorkerHostAndTaskStates(ip, asList(DISPATCHED.getCode(), RUNNING.getCode(), KILL_FAIL.getCode()));
        if (runningTasks != null) {
            tasks = runningTasks.stream()
                                .map(SchedulerTaskBo::from)
                                .collect(toList());
        }
        return tasks;
    }

    public List<SchedulerTaskBo> getTasksByState(TaskState... states) {
        List<SchedulerTaskBo> tasks = emptyList();
        List<SchedulerTask> runningTasks = schedulerTaskRepository
                .findTasksByTaskStates(asList(states).stream()
                                                     .map(TaskState::getCode)
                                                     .collect(toList()));
        if (runningTasks != null) {
            tasks = runningTasks.stream()
                                .map(SchedulerTaskBo::from)
                                .collect(toList());
        }
        return tasks;
    }

    public SchedulerNodeBo saveNode(SchedulerNodeBo worker) {
        requireNonNull(worker, "Worker is null");
        requireNonNull(worker.getHost(), "Worker host is null");
        requireNonNull(worker.getPort(), "Worker port is null");

        SchedulerNode workerFromDb = schedulerNodeRepository.findWorkerByHostAndPort(worker.getHost(), worker.getPort());
        SchedulerNode schedulerNode = worker.toSchedulerNode();
        if (workerFromDb == null) {
            schedulerNode.setCreateTime(now())
                         .setUpdateTime(now());
            return SchedulerNodeBo.from(schedulerNodeRepository.save(schedulerNode));
        }

        return SchedulerNodeBo.from(schedulerNodeRepository.save(workerFromDb.updateNotNull(schedulerNode)));
    }

    public void updateWorkerDead(String ip, Integer port) {
        SchedulerNode workerFromDb = schedulerNodeRepository.findWorkerByHostAndPort(ip, port);
        if (workerFromDb != null) {
            workerFromDb.setNodeState(DEAD.getCode());
            schedulerNodeRepository.save(workerFromDb);
        }
    }

    public List<JobRelation> getJobRelations() {
        List<JobRelation> relations = emptyList();
        try {
            String sql = "select job_id, parent_job_id " +
                    "from t_arlo_scheduler_job_relation a " +
                    "join t_arlo_scheduler_job b on a.job_id = b.id " +
                    "where b.job_release_state in (" + OFFLINE.getCode() + ", " + ONLINE.getCode() + ")";

            relations = jdbcTemplate.query(sql, new BeanPropertyRowMapper(JobRelation.class));
        } catch (Exception e) {
            log.error("Get job relations fail", e);
        }
        return relations;
    }

    public List<TaskSuccessRecord> getLatestSuccessTasks(Long jobId, Integer size) {
        List<TaskSuccessRecord> history = emptyList();
        try {
            history = jdbcTemplate.query(SQL_LATEST_SUCCESS_TASKS, new Object[]{jobId, size},
                    (rs, i) -> TaskSuccessRecord.of(
                            rs.getLong("id"),
                            rs.getString("schedule_cron"),
                            rs.getTimestamp("schedule_time")
                              .toLocalDateTime(),
                            rs.getLong("execution_cost")
                    )
            );
        } catch (Exception e) {
            log.error("Get latest success tasks of job {} limit {} fail", jobId, size, e);
        }
        return history;
    }

    public SchedulerTaskBo getTask(Long taskId) {
        SchedulerTask schedulerTask = schedulerTaskRepository.findById(taskId)
                                                             .orElseThrow(() -> new TaskNotFoundException(taskId));
        return from(schedulerTask);
    }

    public List<String> getLatestFailedHosts(Long jobId, Integer size) {
        List<String> hosts = emptyList();
        try {
            String sql = "select worker_host " +
                    "from t_arlo_scheduler_task " +
                    "where job_id = " + jobId + " " +
                    "and worker_host is not null " +
                    "and task_state = " + FAIL.getCode() + " " +
                    "order by update_time desc " +
                    "limit " + size;
            hosts = jdbcTemplate.query(sql, (rs, i) -> rs.getString("worker_host"));
        } catch (Exception e) {
            log.error("Get latest failed hosts of job {} limit {} fail", jobId, size, e);
        }
        return hosts;
    }

    public boolean isTaskTheFirstInstanceOfJob(Long jobId, LocalDateTime scheduleTime) {
        boolean isFirst = false;
        try {
            String sql = "select count(1) counts " +
                    "from t_arlo_scheduler_task " +
                    "where job_id = " + jobId + " " +
                    "and schedule_time < '" + localDateTimeToStr(scheduleTime) + "' ";
            isFirst = jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("counts")) == 0;
        } catch (Exception e) {
            log.error("Check schedule time {} if the first of job {}", scheduleTime, jobId, e);
        }

        return isFirst;
    }

    public boolean isJobSelfDependent(Long jobId) {
        boolean is_self_dependent = false;
        try {
            String sql = "select is_self_dependent " +
                    "from t_arlo_scheduler_job " +
                    "where id = " + jobId;
            is_self_dependent = jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("is_self_dependent")) == 1;
        } catch (Exception e) {
            log.error("Check job {} if the self depend", jobId, e);
        }

        return is_self_dependent;
    }

    public List<Long> getJobConcurrentTasks(Long jobId) {
        List<Long> tasks = emptyList();
        try {
            String sql = "select id " +
                    "from t_arlo_scheduler_task " +
                    "where job_id = " + jobId + " " +
                    "and task_state in (" + DISPATCHED.getCode() + "," + RUNNING.getCode() + ")";

            tasks = jdbcTemplate.query(sql, (rs, i) -> rs.getLong("id"));
        } catch (Exception e) {
            log.error("Get job {} running tasks fail", jobId, e);
        }
        return tasks;
    }

    public List<Long> getSourceHostConcurrentTasks(String sourceHost) {
        List<Long> tasks = emptyList();
        try {
            String sql = "select id " +
                    "from t_arlo_scheduler_task " +
                    "where source_host = '" + sourceHost + "' " +
                    "and task_state in (" + DISPATCHED.getCode() + "," + RUNNING.getCode() + ")";

            tasks = jdbcTemplate.query(sql, (rs, i) -> rs.getLong("id"));
        } catch (Exception e) {
            log.warn("Get source host {} running tasks fail", sourceHost, e);
        }
        return tasks;
    }

    public Optional<Long> getJobIdByTaskId(Long taskId) {
        Long jobId = null;
        try {
            String sql = "select job_id " +
                    "from t_arlo_scheduler_task " +
                    "where id = " + taskId + " " +
                    "limit 1";

            jobId = jdbcTemplate.queryForObject(sql, Long.class);
        } catch (Exception e) {
            log.error("Get job id by task id {} tasks fail", taskId, e);
        }
        return Optional.ofNullable(jobId);
    }

    public Set<SchedulerDagBo> getUncompletedDags(DagState... states) {
        Set<SchedulerDagBo> result = emptySet();

        List<Integer> statesList = stream(states).map(DagState::getCode)
                                                 .collect(toList());
        List<SchedulerDag> loadBatches =
                schedulerDagRepository.getDagsByStatesAndType(statesList, asList(DYNAMIC.getCode(), STATIC.getCode()));

        if (loadBatches != null) {
            result = loadBatches.stream()
                                .map(SchedulerDagBo::from)
                                .collect(toSet());
        }

        return result;
    }

    public SchedulerDagBo getDag(Long dagId) {
        return SchedulerDagBo.from(this.schedulerDagRepository.findById(dagId)
                                                              .get());
    }

    public boolean dagHasJobSuccessOrUnfinished(Long dagId, Long jobId, LocalDateTime scheduleTime) {
        boolean has = false;
        List<Integer> filterStates = unFinishedStateCodes();
        filterStates.add(SUCCESS.getCode());
        try {
            String sql = "select count(1) counts " +
                    "from t_arlo_scheduler_task " +
                    "where dag_id = " + dagId + " " +
                    "and job_id = " + jobId + " " +
                    "and task_state in (" + on(",").join(filterStates) + ") " +
                    "and schedule_time = str_to_date('" + localDateTimeToStr(scheduleTime) + "', '%Y-%m-%d %H:%i:%s')";
            has = jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("counts")) > 0;
        } catch (Exception e) {
            log.error("Check if dag {} job {} at schedule time {} have success record fail", dagId, jobId, scheduleTime, e);
        }

        return has;
    }

    public Optional<Long> getDuplicateTask(Long jobId, LocalDateTime scheduleTime) {
        try {
            String sql = "select id " +
                    "from t_arlo_scheduler_task " +
                    "where job_id = " + jobId + " " +
                    "and task_state not in (" + finishTaskState() + ") " +
                    "and schedule_time = str_to_date('" + localDateTimeToStr(scheduleTime) + "', '%Y-%m-%d %H:%i:%s') " +
                    "order by id desc limit 1";
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getLong("id")));
        } catch (EmptyResultDataAccessException e) {
            log.debug("Job has no duplicate task");
        } catch (Exception e) {
            log.error("Check job {} at schedule time {} duplication fail", jobId, scheduleTime, e);
        }
        return Optional.empty();
    }

    public Set<LocalDateTime> getDuplicateScheduleTimes(Long jobId) {
        try {
            String sql = "select schedule_time " +
                    "from t_arlo_scheduler_task " +
                    "where job_id = " + jobId + " " +
                    "and task_state not in (" + finishTaskState() + ") ";

            return jdbcTemplate.queryForList(sql)
                               .stream()
                               .map(i -> ((Timestamp) i.get("schedule_time")).toLocalDateTime())
                               .collect(Collectors.toSet());

        } catch (EmptyResultDataAccessException e) {
            log.debug("Job has no duplicate task");
        } catch (Exception e) {
            log.error("Get job {} duplication fail", jobId, e);
        }
        return ImmutableSet.of();
    }

    private String finishTaskState() {
        return on(",").join(TaskState.finishStateCodes());
    }

    public String getJobCron(Long jobId) {
        String cron = null;
        try {
            String sql = "select schedule_cron " +
                    "from t_arlo_scheduler_job " +
                    "where id = " + jobId;

            cron = jdbcTemplate.queryForObject(sql, String.class);
        } catch (Exception e) {
            log.error("Get job {} cron expression fail", cron, e);
        }

        return cron;
    }

    public Set<Long> getJobChildren(Long jobId) {
        List<SchedulerJobRelation> children = this.schedulerJobRelationRepository.findChildrenByParentJobId(jobId);
        if (children == null) return emptySet();
        return children.stream()
                       .map(SchedulerJobRelation::getJobId)
                       .collect(toSet());
    }

    public void updateTaskDependencies(Long taskId, String taskDependencies) {
        try {
            String sql = "update t_arlo_scheduler_task set dependencies_json = '" + taskDependencies + "' " +
                    "where id = " + taskId;

            jdbcTemplate.update(sql);
        } catch (Exception e) {
            log.error("Update task {} dependencies json {} fail", taskId, taskDependencies, e);
        }
    }

    public void updateTaskState(Long taskId, TaskState state) {
        requireNonNull(state, "State should not be null.");

        try {
            String sql = "update t_arlo_scheduler_task set task_state = " + state.getCode() + " " +
                    "where id = " + taskId;

            jdbcTemplate.update(sql);
        } catch (Exception e) {
            log.error("Update task {} state {} fail", taskId, state, e);
        }
    }

    public TaskState getTaskStateById(Long taskId) {
        try {
            String sql = "select task_state from t_arlo_scheduler_task where id = " + taskId;
            Integer taskStateCode = jdbcTemplate.queryForObject(sql, Integer.class);
            return TaskState.of(taskStateCode);
        } catch (Exception e) {
            log.error("Get task_{} state fail", taskId, e);
        }
        return UNKNOWN;
    }

    public boolean isDagFinished(String dagName, int index) {
        try {
            String sql = "select count(1) counts " +
                    "from t_arlo_scheduler_dag " +
                    "where dag_name = '" + dagName + "' " +
                    "and dag_state in (" + on(",").join(finishedStateCodes()) + ") " +
                    "and `batch_index` = " + index;

            return jdbcTemplate.queryForObject(sql, (rs, i) -> rs.getInt("counts")) > 0;
        } catch (Exception e) {
            log.error("Check dag {} batchIndex {} if completed fail", dagName, index, e);
        }

        return false;
    }
}
