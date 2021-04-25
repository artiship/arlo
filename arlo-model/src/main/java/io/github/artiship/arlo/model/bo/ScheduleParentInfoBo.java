package io.github.artiship.arlo.model.bo;

import io.github.artiship.arlo.model.entity.SchedulerJob;
import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Data
public class ScheduleParentInfoBo {

    private Long jobId;
    private String ownerNames;
    private String jobName;
    private String scheduleCron;

    public static ScheduleParentInfoBo from(SchedulerJob job) {
        return new ScheduleParentInfoBo()
                .setOwnerNames(job.getOwnerNames())
                .setScheduleCron(job.getScheduleCron())
                .setJobName(job.getJobName())
                .setJobId(job.getId());

    }
}
