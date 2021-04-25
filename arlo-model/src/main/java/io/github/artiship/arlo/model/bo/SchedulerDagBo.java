package io.github.artiship.arlo.model.bo;

import com.google.common.collect.ComparisonChain;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.github.artiship.arlo.model.entity.SchedulerDag;
import io.github.artiship.arlo.model.enums.DagNodeType;
import io.github.artiship.arlo.model.enums.DagState;
import io.github.artiship.arlo.model.enums.DagType;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import static io.github.artiship.arlo.model.enums.DagState.SUBMITTED;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

@Data
@Accessors(chain = true)
public class SchedulerDagBo implements Comparable<SchedulerDagBo> {
    private static Gson gson = new Gson();
    private static Type treeNodeSetType = new TypeToken<Set<DagNode>>() {
    }.getType();
    @Id
    private Long id;
    private Long startId;
    private String dagName;
    private Integer batchIndex;
    private DagNodeType dagNodeType;    //job or task
    private DagType dagType;    //static or dynamic
    private Set<DagNode> dag = new HashSet<>(); // start job id or task id
    private LocalDateTime scheduleTime;
    private LocalDateTime startScheduleTime;
    private LocalDateTime endScheduleTime;
    private Integer parallelism;    //default is 1
    private String submitterId;
    private String submitterName;
    private DagState dagState;
    private LocalDateTime creatTime;
    private LocalDateTime updateTime;
    private Integer penalty = 0;

    public static SchedulerDagBo from(SchedulerDag schedulerDag) {
        return new SchedulerDagBo().setId(schedulerDag.getId())
                                   .setStartId(schedulerDag.getStartId())
                                   .setDagName(schedulerDag.getDagName())
                                   .setBatchIndex(schedulerDag.getBatchIndex())
                                   .setDagType(DagType.of(schedulerDag.getDagType()))
                                   .setDagNodeType(DagNodeType.of(schedulerDag.getDagNodeType()))
                                   .setDag(parseDagJson(schedulerDag.getDagJson()))
                                   .setScheduleTime(schedulerDag.getScheduleTime())
                                   .setDagState(DagState.of(schedulerDag.getDagState()))
                                   .setParallelism(schedulerDag.getParallelism())
                                   .setSubmitterId(schedulerDag.getSubmitterId())
                                   .setSubmitterName(schedulerDag.getSubmitterName())
                                   .setCreatTime(schedulerDag.getCreateTime())
                                   .setUpdateTime(schedulerDag.getUpdateTime());
    }

    public static Set<DagNode> parseDagJson(String dagJson) {
        if (dagJson == null || dagJson.length() == 0)
            return emptySet();

        Set<DagNode> treeNodes = gson.fromJson(dagJson, treeNodeSetType);

        if (treeNodes != null) return treeNodes;

        return emptySet();
    }

    public static String toDagJson(Set<DagNode> dag) {
        if (dag == null)
            return "";

        return gson.toJson(dag);
    }

    public SchedulerDag toSchedulerDag() {
        requireNonNull(this.dagNodeType, "Dag node type is null");
        requireNonNull(this.dagType, "Dag node type is null");

        return new SchedulerDag().setId(this.id)
                                 .setStartId(this.startId)
                                 .setDagName(this.dagName)
                                 .setBatchIndex(this.batchIndex)
                                 .setDagType(this.dagType.getCode())
                                 .setDagNodeType(this.dagNodeType.getCode())
                                 .setDagJson(toDagJson(this.dag))
                                 .setScheduleTime(this.scheduleTime)
                                 .setParallelism(this.parallelism == null ? Integer.valueOf(1) : this.parallelism)
                                 .setSubmitterId(this.submitterId)
                                 .setSubmitterName(this.submitterName)
                                 .setDagState(this.dagState == null ? SUBMITTED.getCode() : this.dagState.getCode())
                                 .setCreateTime(this.creatTime)
                                 .setUpdateTime(this.updateTime);
    }

    public void addTreeNode(DagNode treeNode) {
        this.dag.add(treeNode);
    }

    @Override
    public int compareTo(SchedulerDagBo that) {
        return ComparisonChain.start()
                              .compare(this.penalty, that.penalty)
                              .compare(this.dagName, that.dagName)
                              .compare(this.scheduleTime, that.scheduleTime)
                              .result();
    }

    public SchedulerDagBo penalty() {
        this.penalty++;
        return this;
    }

    public boolean isFirstOfTheBatch() {
        if (this.batchIndex == null || this.batchIndex == 0) return true;
        return false;
    }

    public boolean shouldRunSerial() {
        if (this.parallelism == 1) return true;
        return false;
    }
}
