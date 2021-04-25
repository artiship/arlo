package io.github.artiship.arlo.db.repository;

import io.github.artiship.arlo.model.entity.SchedulerTask;
import io.github.artiship.arlo.model.vo.JobTaskState;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public interface SchedulerTaskRepository extends PagingAndSortingRepository<SchedulerTask, Long> {

    @Query(rowMapperClass = RunningJobCountMapper.class, value = "select worker,count(*) as cnt from t_arlo_scheduler_task where worker is not null group by worker")
    List<Map<String, Object>> getRunningJobCount();

    @Query("select * from t_arlo_scheduler_task where job_id = :jobId and schedule_time = :scheduleTime order by start_time desc")
    List<SchedulerTask> getInstancesByIdAndTime(Long jobId, String scheduleTime);

    @Query("select * from t_arlo_scheduler_task where worker_host = :ip and task_state in (:states)")
    List<SchedulerTask> findAllTaskByWorkerHostAndTaskStates(String ip, List<Integer> states);

    @Query(rowMapperClass = AllJobStatusMapper.class, value = "select id,job_id,task_state,schedule_time from t_arlo_scheduler_task where schedule_time is not null and schedule_time and schedule_time > :startTime and schedule_time < :endTime")
    List<JobTaskState> getAllJobStatus(String startTime, String endTime);

    @Query("select * from t_arlo_scheduler_task where task_state in (:states) order by schedule_time asc")
    List<SchedulerTask> findTasksByTaskStates(List<Integer> states);

    @Query("select * from t_arlo_scheduler_task where job_id = :jobId and task_state in (:states)")
    List<SchedulerTask> findTasksByTaskStatesAndJobId(Long jobId, List<Integer> states);

    @Query("select * from t_arlo_scheduler_task where dag_id = :dagId order by create_time")
    List<SchedulerTask> findAllByDagId(Long dagId);

    @Modifying
    @Query("update t_arlo_scheduler_task set task_state = :state where id in (:ids)")
    Integer updateJobStatus(List<Long> ids, Integer state);

    @Modifying
    @Query("delete from t_arlo_scheduler_task where job_id = :jobId")
    Integer deleteAllByJobId(Long jobId);

    @Modifying
    @Query("update t_arlo_scheduler_task set task_state = :state where job_id = :id and schedule_time = :scheduleTime")
    Integer updateJobStatusAsFailed(Integer state, Long jobId, String scheduleTime);

    @Query("select * from t_arlo_scheduler_task where job_id = :jobId and schedule_time < :scheduleTime order by schedule_time, start_time")
    List<SchedulerTask> getTaskList(Long jobId, String scheduleTime);

    @Query("select b.* from (select max(id) as id from t_arlo_scheduler_task  where job_id = :id and schedule_time =:time group by schedule_time ,job_id ) a  join  ( select * from t_arlo_scheduler_task where job_id  = :id and schedule_time = :time ) b on a.id = b.id")
    List<SchedulerTask> findTaskByChildTime(Long id, String time);

    @Query("select * from t_arlo_scheduler_task where job_id = :jobId and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :executionDate")
    List<SchedulerTask> findByExecutionDateByHour(Long jobId, String executionDate);

    @Query("select * from t_arlo_scheduler_task where job_id = :jobId and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :executionDate")
    List<SchedulerTask> findByExecutionDateByDay(Long jobId, String executionDate);

    @Query(rowMapperClass = CheckReadyMapper.class, value = "select id as task_id, task_state, schedule_time, start_time, end_time " +
            "from t_arlo_scheduler_task " +
            "where task_name = :tableName " +
            "and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :day " +
            "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByDay(String tableName, String day);

    @Query(rowMapperClass = CheckReadyMapper.class, value = "select id as task_id, task_state, schedule_time, start_time, end_time " +
            "from t_arlo_scheduler_task " +
            "where task_name = :tableName " +
            "and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :dayHour " +
            "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByHour(String tableName, String dayHour);

    @Query(rowMapperClass = CheckReadyMapper.class, value = "select id as task_id, task_state, schedule_time, start_time, end_time " +
            "from t_arlo_scheduler_task " +
            "where job_id = :jobId " +
            "and DATE_FORMAT(schedule_time,'%Y-%m-%d') = :day " +
            "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByJobIdAndDay(Long jobId, String day);

    @Query(rowMapperClass = CheckReadyMapper.class, value = "select id as task_id, task_state, schedule_time, start_time, end_time " +
            "from t_arlo_scheduler_task " +
            "where job_id = :jobId " +
            "and DATE_FORMAT(schedule_time,'%Y-%m-%d-%H') = :dayHour " +
            "order by update_time desc limit 1")
    List<Map<String, Object>> checkReadyByJobIdAndHour(Long jobId, String dayHour);

    class CheckReadyMapper implements RowMapper<Map<String, Object>> {
        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            HashMap<String, Object> result = new HashMap<>();
            result.put("taskId", rs.getLong("task_id"));
            result.put("taskState", rs.getInt("task_state"));
            result.put("scheduleTime", rs.getTimestamp("schedule_time"));
            result.put("startTime", rs.getTimestamp("start_time"));
            result.put("endTime", rs.getTimestamp("end_time"));
            return result;
        }
    }

    class RunningJobCountMapper implements RowMapper<Map<String, Object>> {
        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            HashMap<String, Object> result = new HashMap<>();
            result.put("worker", rs.getString("worker"));
            result.put("cnt", rs.getLong("cnt"));
            return result;
        }
    }

    class AllJobStatusMapper implements RowMapper<JobTaskState> {
        @Override
        public JobTaskState mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobTaskState jobTaskState = new JobTaskState();
            jobTaskState.setId(rs.getInt("id"));
            jobTaskState.setJobId(rs.getInt("job_id"));
            jobTaskState.setScheduleTime(rs.getDate("schedule_time"));
            jobTaskState.setTaskState(rs.getInt("task_state"));
            return jobTaskState;
        }
    }
}
