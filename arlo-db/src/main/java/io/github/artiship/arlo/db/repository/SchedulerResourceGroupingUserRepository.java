package io.github.artiship.arlo.db.repository;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SchedulerResourceGroupingUserRepository extends CrudRepository<SchedulerResourceGroupingUser, Long> {

    @Query("select * from t_arlo_scheduler_resource_grouping_user where group_id  = :id order by id desc")
    List<SchedulerResourceGroupingUser> findAllUserByGroupId(Long id);

    @Query("select count(1) as num from t_arlo_scheduler_resource_grouping_user where group_id  = :groupId and user_id =:userId")
    long findUserByGroupIdAndUserId(Long groupId, Long userId);

    @Modifying
    @Query("delete from t_arlo_scheduler_resource_grouping_user where group_id = :groupId and user_id = :userId")
    Integer deleteByGroupIdAndUserId(Long groupId, Long userId);
}
