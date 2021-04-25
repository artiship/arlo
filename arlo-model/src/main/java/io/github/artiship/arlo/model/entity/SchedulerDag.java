package io.github.artiship.arlo.model.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Accessors(chain = true)
@Data
@Table("t_arlo_scheduler_dag")
public class SchedulerDag {

    @Id
    private Long id;
    private Long startId;
    private Integer dagType; // static or dynamic
    private Integer dagNodeType; // job or task
    private String dagName;
    private Integer batchIndex;
    private String dagJson; // job relations
    private LocalDateTime scheduleTime;
    private LocalDateTime scheduleStartTime;
    private LocalDateTime scheduleEndTime;
    private Integer parallelism;    //default is 1
    private String submitterId;
    private String submitterName;
    private Integer dagState;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public SchedulerDag updateIgnoreNull(SchedulerDag dag) {
        if (dag.getId() != null) this.id = dag.getId();
        if (dag.getStartId() != null) this.startId = dag.getStartId();
        if (dag.getDagType() != null) this.dagType = dag.getDagType();
        if (dag.getDagType() != null) this.dagNodeType = dag.getDagNodeType();
        if (dag.getDagName() != null) this.dagName = dag.getDagName();
        if (dag.getBatchIndex() != null) this.batchIndex = dag.getBatchIndex();
        if (dag.getDagJson() != null) this.dagJson = dag.getDagJson();
        if (dag.getScheduleTime() != null) this.scheduleTime = dag.getScheduleTime();
        if (dag.getScheduleStartTime() != null) this.scheduleStartTime = dag.getScheduleStartTime();
        if (dag.getScheduleEndTime() != null) this.scheduleEndTime = dag.getScheduleEndTime();
        if (dag.getParallelism() != null) this.parallelism = dag.getParallelism();
        if (dag.getSubmitterId() != null) this.submitterId = dag.getSubmitterId();
        if (dag.getSubmitterName() != null) this.submitterName = dag.getSubmitterName();
        if (dag.getDagState() != null) this.dagState = dag.getDagState();
        if (dag.getCreateTime() != null) this.createTime = dag.getCreateTime();
        if (dag.getUpdateTime() != null) this.updateTime = dag.getUpdateTime();

        return this;
    }
}
