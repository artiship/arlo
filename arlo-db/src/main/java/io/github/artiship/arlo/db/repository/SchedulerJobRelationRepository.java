package io.github.artiship.arlo.db.repository;


import io.github.artiship.arlo.model.entity.SchedulerJobRelation;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SchedulerJobRelationRepository extends CrudRepository<SchedulerJobRelation, Long> {

    @Modifying
    @Query("delete from t_arlo_scheduler_job_relation where job_id = :id")
    Integer deleteByJobId(Long jobId);

    @Query("select * from t_arlo_scheduler_job_relation where job_id = :jobId")
    List<SchedulerJobRelation> findParentsByJobId(Long jobId);

    @Query("select * from t_arlo_scheduler_job_relation where job_id = :jobId and parent_job_id != -1")
    List<SchedulerJobRelation> findParentsByJobIdNoParent(Long jobId);

    @Query("select * from t_arlo_scheduler_job_relation where parent_job_id= :parentJobId")
    List<SchedulerJobRelation> findChildrenByParentJobId(Long parentJobId);

    @Query("select count(1) from t_arlo_scheduler_job_relation where parent_job_id= :parentJobId")
    Integer findChildrenCountByParentJobId(Long parentJobId);

    @Query("select * from t_arlo_scheduler_job_relation where job_id = :jobId and parent_job_id= :parentJobId")
    List<SchedulerJobRelation> findIdByJobIdAndParentJobId(Long jobId, Long parentJobId);

    @Query("select * from t_arlo_scheduler_job_relation")
    List<SchedulerJobRelation> findAllRelation();

    @Query("select count(1) as num from t_arlo_scheduler_job_relation where job_id =:jobId and parent_job_id =:parentJobId")
    long findRelationByJobIdAndParentId(Long jobId, Long parentJobId);
}
