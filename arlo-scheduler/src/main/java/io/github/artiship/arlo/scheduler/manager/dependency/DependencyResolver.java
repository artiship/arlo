package io.github.artiship.arlo.scheduler.manager.dependency;

import java.time.LocalDateTime;
import java.util.List;

public interface DependencyResolver {
    public List<LocalDateTime> parentScheduleTimes();
}
