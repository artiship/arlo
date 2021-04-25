package io.github.artiship.arlo.db.repository;

import io.github.artiship.arlo.model.entity.SchedulerNode;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SchedulerNodeRepository extends CrudRepository<SchedulerNode, Long> {

    @Modifying
    @Query("update t_arlo_scheduler_node set node_status = :status where node_group= :nodeGroup and node_host in (:worker)")
    Integer updateNodesStatusByGroup(Integer status, String nodeGroup, List<String> worker);

    @Query("select * from t_arlo_scheduler_node where node_group= :nodeGroup and node_host in (:worker)")
    List<SchedulerNode> getNodesInfoByGroup(String nodeGroup, List<String> worker);

    @Query("select * from t_arlo_scheduler_node where node_host = :host and node_rpc_port = :rpcPort")
    SchedulerNode findWorkerByHostAndPort(String host, Integer rpcPort);
}
