package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.scheduler.model.dependency.DependencyRange;
import io.github.artiship.arlo.scheduler.model.dependency.DependencyRule;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;

@Builder
public class CustomizeDependencyResolver implements DependencyResolver {
    private final String parentCronExpression;
    private final DependencyRule dependencyRule;
    private final DependencyRange dependencyRange;
    private final boolean isParentSelfDepend;

    @Override
    public List<LocalDateTime> parentScheduleTimes() {
        return null;
    }
}
