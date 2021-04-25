package io.github.artiship.arlo.scheduler.core.model;

import com.google.common.base.Splitter;
import io.github.artiship.arlo.model.entity.SchedulerNode;
import io.github.artiship.arlo.model.enums.NodeState;
import io.github.artiship.arlo.model.enums.NodeType;
import io.github.artiship.arlo.scheduler.core.rpc.api.RpcHeartbeat;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Lists.newArrayList;
import static io.github.artiship.arlo.constants.GlobalConstants.DEAD_WORKER_GROUP;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static org.springframework.util.CollectionUtils.containsAny;

@Data
@Accessors(chain = true)
public class SchedulerNodeBo implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private NodeType nodeType;
    private String host;
    private Integer port;
    private List<String> workerGroups;
    private Double cpuUsage;
    private Double memoryUsage;
    private Integer maxTasks;
    private Integer runningTasks;
    private LocalDateTime lastHeartbeatTime;
    private NodeState nodeState;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public static SchedulerNodeBo from(RpcHeartbeat heartbeat) {
        return new SchedulerNodeBo().setPort(heartbeat.getPort())
                                    .setHost(heartbeat.getHost())
                                    .setWorkerGroups(heartbeat.getWorkerGroupsList() == null ? null
                                            : newArrayList(heartbeat.getWorkerGroupsList()))
                                    .setMaxTasks(heartbeat.getMaxTask())
                                    .setMemoryUsage(heartbeat.getMemoryUsage())
                                    .setCpuUsage(heartbeat.getCpuUsage())
                                    .setLastHeartbeatTime(localDateTime(heartbeat.getTime()))
                                    .setRunningTasks(heartbeat.getRunningTasks());
    }

    public static SchedulerNodeBo from(SchedulerNode schedulerNode) {
        return new SchedulerNodeBo().setId(schedulerNode.getId())
                                    .setNodeState(schedulerNode.getNodeState() == null ? null
                                            : NodeState.of(schedulerNode.getNodeState()))
                                    .setHost(schedulerNode.getNodeHost())
                                    .setPort(schedulerNode.getNodeRpcPort())
                                    .setCpuUsage(schedulerNode.getCpuUsage())
                                    .setMemoryUsage(schedulerNode.getMemoryUsage())
                                    .setMaxTasks(schedulerNode.getMaxTasks())
                                    .setRunningTasks(schedulerNode.getRunningTasks())
                                    .setLastHeartbeatTime(schedulerNode.getLastHeartbeatTime())
                                    .setCreateTime(schedulerNode.getCreateTime())
                                    .setUpdateTime(schedulerNode.getUpdateTime())
                                    .setWorkerGroups(schedulerNode.getNodeGroups() == null ? null
                                            : Splitter.on(",")
                                                      .splitToList(schedulerNode.getNodeGroups()));
    }

    public boolean isActive() {
        return nodeState == NodeState.ACTIVE;
    }

    public SchedulerNode toSchedulerNode() {
        return new SchedulerNode().setId(this.getId())
                                  .setNodeRpcPort(this.port)
                                  .setNodeHost(this.host)
                                  .setCpuUsage(this.cpuUsage)
                                  .setMemoryUsage(this.memoryUsage)
                                  .setMaxTasks(this.maxTasks)
                                  .setRunningTasks(this.runningTasks)
                                  .setNodeGroups(this.getNodeGroupStr())
                                  .setNodeState(this.getNodeStateCode())
                                  .setCreateTime(this.createTime)
                                  .setUpdateTime(this.updateTime)
                                  .setLastHeartbeatTime(this.lastHeartbeatTime);
    }

    public Integer getNodeTypeCode() {
        return this.nodeType == null ? null : this.nodeType.getCode();
    }

    public Integer getNodeStateCode() {
        return this.nodeState == null ? null : this.nodeState.getCode();
    }

    public String getNodeGroupStr() {
        return this.workerGroups == null ? null : on(",").join(workerGroups);
    }

    public Integer getMaxTasks() {
        return this.maxTasks == null ? Integer.valueOf(0) : this.maxTasks;
    }

    public Integer getRunningTasks() {
        return this.runningTasks == null ? Integer.valueOf(0) : this.runningTasks;
    }

    public boolean hasTaskSlots() {
        return (this.getMaxTasks() - this.getRunningTasks()) > 0;
    }

    public Integer availableSlots() {
        return this.getMaxTasks() - this.getRunningTasks();
    }

    public boolean inWorkerGroups(List<String> workerGroups) {
        if (workerGroups == null || workerGroups.isEmpty()) return true;
        if (this.workerGroups == null) return false;
        return containsAny(this.workerGroups, workerGroups);
    }

    public boolean notInFailedList(List<String> lastFailedHosts) {
        if (lastFailedHosts == null || lastFailedHosts.isEmpty()) return true;
        if (lastFailedHosts.contains(this.host)) return false;
        return true;
    }

    public String getIpAndPortStr() {
        return this.getHost() + ":" + this.getPort();
    }

    public String getDeadWorkerPath() {
        return DEAD_WORKER_GROUP + "/" + this.getIpAndPortStr();
    }
}