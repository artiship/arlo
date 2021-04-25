package io.github.artiship.arlo.model.vo;

import io.github.artiship.arlo.model.entity.SchedulerJobRelation;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Accessors(chain = true)
@Data
public class JobRelation {
    private Long jobId;
    private Long parentJobId;

    public JobRelation(Long jobId, Long parentJobId) {
        requireNonNull(parentJobId, "Parent job id is null");
        requireNonNull(jobId, "Child job id is null");

        this.jobId = jobId;
        this.parentJobId = parentJobId;
    }

    public JobRelation() {
    }

    public static JobRelation of(Long jobId, Long parentJobId) {
        return new JobRelation(jobId, parentJobId);
    }

    public static List<JobRelation> from(List<SchedulerJobRelation> schedulerJobRelations) {
        List<JobRelation> collect = schedulerJobRelations.stream()
                                                         .map(e -> new JobRelation()
                                                                 .setJobId(e.getJobId())
                                                                 .setParentJobId(e.getParentJobId()))
                                                         .collect(Collectors.toList());
        return collect;
    }

    public String networkEdge() {
        return this.parentJobId + "->" + this.jobId;
    }
}
