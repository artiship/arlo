package io.github.artiship.arlo.model.vo;

import io.github.artiship.arlo.model.bo.DagNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ComplementDownStreamRequest {
    private Long jobId;
    private String startTime;
    private String endTime;
    private Integer parallelism;
    private String submitterId;
    private String submitterName;
    private Set<DagNode> dag;

}


