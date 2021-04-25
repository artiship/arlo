package io.github.artiship.arlo.db.repository;

import io.github.artiship.arlo.model.entity.SchedulerDag;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SchedulerDagRepository extends CrudRepository<SchedulerDag, Long> {
    @Query("select * from t_arlo_scheduler_dag where dag_state in (:dagStates)")
    List<SchedulerDag> getDagsByStates(List<Integer> dagStates);

    @Query("select * from t_arlo_scheduler_dag where dag_state in (:dagStates) and dag_type in (:dagTypes)")
    List<SchedulerDag> getDagsByStatesAndType(List<Integer> dagStates, List<Integer> dagTypes);

    @Query("select * from t_arlo_scheduler_dag order by update_time desc limit :offset, :size")
    List<SchedulerDag> findAll(long offset, long size);

    @Query("select count(1) from t_arlo_scheduler_dag")
    Long getTotal();
}
