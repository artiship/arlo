package io.github.artiship.arlo.model.vo;

import io.github.artiship.arlo.model.entity.SchedulerJob;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@Accessors(chain = true)
public class SchedulerDownStreamVO {

    private String jobName;
    private Integer jobType;
    private String cron;
    private String owners;
    private Long jobId;
    private List<SchedulerDownStreamVO> children;

    public static SchedulerDownStreamVO from(SchedulerJob schedulerJob) {
        return new SchedulerDownStreamVO()
                .setJobId(schedulerJob.getId())
                .setCron(schedulerJob.getScheduleCron())
                .setJobName(schedulerJob.getJobName())
                .setJobType(schedulerJob.getJobType())
                .setOwners(schedulerJob.getOwnerNames());
    }
}
