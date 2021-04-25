package io.github.artiship.arlo.model.bo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DagNode implements Comparable<DagNode> {
    private Long id;
    private Set<Long> children;
    private boolean ignore = false;

    public DagNode(Long id) {
        this.id = id;
        this.children = new HashSet<>();
    }

    public void add(Long childId) {
        children.add(childId);
    }

    @Override
    public int compareTo(DagNode o) {
        return this.id.compareTo(o.getId());
    }

}
