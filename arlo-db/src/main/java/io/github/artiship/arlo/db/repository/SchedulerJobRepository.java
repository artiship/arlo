package io.github.artiship.arlo.db.repository;

import io.github.artiship.arlo.model.entity.SchedulerJob;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SchedulerJobRepository extends CrudRepository<SchedulerJob, Long> {
    @Query("select * " +
            "from t_arlo_scheduler_job a " +
            "left join t_arlo_scheduler_job_relation b on a.id = b.job_id " +
            "where a.job_release_state = 0 " +
            "and a.id in (:ids)")
    List<SchedulerJob> getJobParentsByIds(List<Long> ids);

    @Query("select * from t_arlo_scheduler_job where id = :id and job_release_state = :jobReleaseState")
    SchedulerJob findByIdAndJobReleaseState(Long id, Integer jobReleaseState);

    @Modifying
    @Query("update t_arlo_scheduler_job set job_release_state = :state where id in (:ids)")
    Integer updateJobReleaseStatusByIds(List<Long> ids, Integer state);

    @Modifying
    @Query("update t_arlo_scheduler_job set job_release_state = :state where id =:id")
    Integer updateJobReleaseStatusById(Long id, Integer state);

    @Modifying
    @Query("update t_arlo_scheduler_job set version = :version, oss_path = :ossPath, job_configuration = :versionContent where id =:id")
    Integer updateJobVersionById(Long id, String version, String ossPath, String versionContent);

    @Query("select * from t_arlo_scheduler_job where id in (:ids) and job_release_state = :jobReleaseState")
    List<SchedulerJob> findAllByIdsAndJobReleaseState(List<Long> ids, Integer jobReleaseState);

    @Modifying
    @Query("delete from t_arlo_scheduler_job_relation where job_id = :id and parent_job_id = :parent_job_id")
    Integer deleteByJobId(Long jobId, Long parent_job_id);

    @Query("select * from t_arlo_scheduler_job where job_name = :jobName and job_release_state != -2 ")
    List<SchedulerJob> findJobsByName(String jobName);

    @Query("select * from t_arlo_scheduler_job where job_name = :jobName and job_release_state != -2 and id != :jobId")
    List<SchedulerJob> findJobsByNameAndId(Long jobId, String jobName);

    @Query("select * from t_arlo_scheduler_job where job_release_state != -2 and job_type in (:integers)")
    List<SchedulerJob> findAllByTag(List<Integer> integers);

    @Query("select * from t_arlo_scheduler_job where job_type = 9 and job_release_state != -2 and json_extract(job_configuration,'$.reportId') = :tableauId")
    List<SchedulerJob> queryTableauJobByTableauId(String tableauId);

    @Query("select * from t_arlo_scheduler_job where job_release_state != -2")
    List<SchedulerJob> queryJobs();

    @Query("select count(1) from t_arlo_scheduler_job where job_name = :tableName and job_release_state != -2 ")
    Integer queryCountByTableName(String tableName);
}
