package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.scheduler.core.exception.CronNotSatisfiedException;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

import static io.github.artiship.arlo.model.enums.JobCycle.numberOfCycles;
import static io.github.artiship.arlo.model.enums.JobCycle.truncateUnit;
import static io.github.artiship.arlo.utils.QuartzUtils.*;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Objects.requireNonNull;

@Slf4j
@Data
public class DagDependencyBuilder {
    private final String parentCronExpression;
    private final String childCronExpression;
    private final LocalDateTime parentScheduleTime;

    private DagDependencyBuilder(Builder builder) {
        this.parentCronExpression = builder.getParentCronExpression();
        this.childCronExpression = builder.getChildCronExpression();
        this.parentScheduleTime = builder.getParentScheduleTime();
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<LocalDateTime> childSchedulerTimes() {
        requireNonNull(childCronExpression, "Child cron expression is null.");

        if (!isSatisfiedBy(requireNonNull(parentCronExpression, "Parent cron expression is null."),
                requireNonNull(parentScheduleTime, "Child schedule time is null."))) {
            throw new CronNotSatisfiedException("Cron " + parentCronExpression + " is not satisfied by " + parentScheduleTime);
        }

        long parentInterval = interval(parentCronExpression);

        ChronoUnit truncateUnit = truncateUnit(parentInterval);
        Integer cycles = numberOfCycles(parentInterval);

        LocalDateTime start;
        LocalDateTime end;

        try {
            start = parentScheduleTime.truncatedTo(truncateUnit)
                                      .minus(cycles - 1, truncateUnit);
        } catch (UnsupportedTemporalTypeException e) {
            start = preScheduleTime(parentCronExpression, parentScheduleTime)
                    .truncatedTo(DAYS)
                    .minus(cycles - 1, DAYS);
        }

        try {
            end = nextScheduleTime(parentCronExpression, parentScheduleTime)
                    .truncatedTo(truncateUnit)
                    .minus(cycles - 1, truncateUnit);
        } catch (UnsupportedTemporalTypeException e) {
            end = preScheduleTime(parentCronExpression, nextScheduleTime(parentCronExpression, parentScheduleTime))
                    .truncatedTo(DAYS)
                    .minus(cycles - 1, DAYS);
        }

        return computeScheduleTimes(childCronExpression, start, end);
    }

    @Getter
    public static class Builder {
        private String parentCronExpression;
        private String childCronExpression;
        private LocalDateTime parentScheduleTime;

        public Builder parentCronExpression(String parentCronExpression) {
            this.parentCronExpression = parentCronExpression;
            return this;
        }

        public Builder childCronExpression(String childCronExpression) {
            this.childCronExpression = childCronExpression;
            return this;
        }

        public Builder parentScheduleTime(LocalDateTime parentScheduleTime) {
            this.parentScheduleTime = parentScheduleTime;
            return this;
        }

        public DagDependencyBuilder build() {
            return new DagDependencyBuilder(this);
        }
    }
}
