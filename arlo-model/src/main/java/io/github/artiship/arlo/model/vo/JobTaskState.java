package io.github.artiship.arlo.model.vo;

import lombok.Data;

import java.util.Date;

@Data
public class JobTaskState {

    private int id;
    private int jobId;
    private Date scheduleTime;
    private int TaskState;
}
