package io.github.artiship.arlo.model.vo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
public class ComplementRequest {
    private Long jobId;
    private Long dagId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Integer parallelism;
    private Boolean checkDependency;
}
