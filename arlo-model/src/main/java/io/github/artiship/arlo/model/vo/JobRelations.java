package io.github.artiship.arlo.model.vo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Accessors(chain = true)
@Data
public class JobRelations {
    private List<JobRelation> list;
}
