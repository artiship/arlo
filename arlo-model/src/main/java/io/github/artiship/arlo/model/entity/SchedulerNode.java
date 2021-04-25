package io.github.artiship.arlo.model.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

import static java.time.LocalDateTime.now;

@Data
@Accessors(chain = true)
@Table("t_arlo_scheduler_node")
public class SchedulerNode {
    @Id
    private Long id;
    private String nodeHost;
    private Integer nodeRpcPort;
    private Integer nodeState;
    private String nodeGroups;
    private Double cpuUsage;
    private Double memoryUsage;
    private Integer maxTasks;
    private Integer runningTasks;
    private LocalDateTime lastHeartbeatTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public SchedulerNode updateNotNull(SchedulerNode schedulerNode) {
        if (schedulerNode.getId() != null) this.id = schedulerNode.getId();
        if (schedulerNode.getNodeHost() != null) this.nodeHost = schedulerNode.getNodeHost();
        if (schedulerNode.getNodeRpcPort() != null) this.nodeRpcPort = schedulerNode.getNodeRpcPort();
        if (schedulerNode.getNodeGroups() != null) this.nodeGroups = schedulerNode.getNodeGroups();
        if (schedulerNode.getCpuUsage() != null) this.cpuUsage = schedulerNode.getCpuUsage();
        if (schedulerNode.getMemoryUsage() != null) this.memoryUsage = schedulerNode.getMemoryUsage();
        if (schedulerNode.getNodeState() != null) this.nodeState = schedulerNode.getNodeState();
        if (schedulerNode.getMaxTasks() != null) this.maxTasks = schedulerNode.getMaxTasks();
        if (schedulerNode.getRunningTasks() != null) this.runningTasks = schedulerNode.getRunningTasks();
        if (schedulerNode.getLastHeartbeatTime() != null) this.lastHeartbeatTime = schedulerNode.getLastHeartbeatTime();
        if (schedulerNode.getCreateTime() != null) this.createTime = schedulerNode.getCreateTime();
        this.updateTime = now();
        return this;
    }
}
