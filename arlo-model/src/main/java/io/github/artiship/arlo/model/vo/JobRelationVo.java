package io.github.artiship.arlo.model.vo;

import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(chain = true)

@Data
public class JobRelationVo {

    private Long id;
    private Long jobId;
    private Long parentJobId;


    public static JobRelationVo from(SchedulerJobRelation schedulerJobRelation) {
        return new JobRelationVo()
                .setId(schedulerJobRelation.getId())
                .setJobId(schedulerJobRelation.getJobId())
                .setParentJobId(schedulerJobRelation.getParentJobId());
    }

    public static JobRelationVo of(Long id, Long jobId, Long parentJobId) {
        return new JobRelationVo()
                .setId(id)
                .setJobId(jobId)
                .setParentJobId(parentJobId);
    }
}
