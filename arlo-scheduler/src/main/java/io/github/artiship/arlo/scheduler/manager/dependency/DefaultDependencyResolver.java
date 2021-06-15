package io.github.artiship.arlo.scheduler.manager.dependency;

import io.github.artiship.arlo.scheduler.core.exception.CronNotSatisfiedException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.quartz.CronExpression;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.artiship.arlo.model.enums.JobCycle.numberOfCycles;
import static io.github.artiship.arlo.model.enums.JobCycle.truncateUnit;
import static io.github.artiship.arlo.utils.CronUtils.previousScheduleTimeOf;
import static io.github.artiship.arlo.utils.Dates.date;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static io.github.artiship.arlo.utils.QuartzUtils.*;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.quartz.TriggerUtils.computeFireTimesBetween;

@Slf4j
@Getter
public class DefaultDependencyResolver implements DependencyResolver {
    private final String parentCronExpression;
    private final String childCronExpression;
    private final LocalDateTime childScheduleTime;
    private final boolean isParentSelfDepend;

    private DefaultDependencyResolver(Builder builder) {
        this.parentCronExpression = builder.getParentCronExpression();
        this.childCronExpression = builder.getChildCronExpression();
        this.childScheduleTime = builder.getChildScheduleTime();
        this.isParentSelfDepend = builder.isParentSelfDepend();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public List<LocalDateTime> parentScheduleTimes() {
        requireNonNull(parentCronExpression, "Parent cron expression is null.");
        requireNonNull(childCronExpression, "Child cron expression is null.");
        requireNonNull(childScheduleTime, "Child schedule time is null.");

        if (!isSatisfiedBy(childCronExpression, childScheduleTime)) {
            throw new CronNotSatisfiedException("Cron " + childCronExpression + " is not satisfied by " + childScheduleTime);
        }

        try {
            List<LocalDateTime> scheduleTimes = compute().stream()
                                                         .map(d -> localDateTime(d))
                                                         .collect(Collectors.toList());
            if (scheduleTimes.isEmpty()) {
                return scheduleTimes;
            }

            if (isParentSelfDepend) {
                return asList(scheduleTimes.get(scheduleTimes.size() - 1));
            }

            return scheduleTimes;
        } catch (Exception e) {
            log.warn("Compute {} schedule times fail.", this, e);
            return Lists.emptyList();
        }
    }

    private List<Date> compute() throws Exception {
        List<Date> result = new ArrayList<>();

        CronExpression parentCron = new CronExpression(parentCronExpression);
        CronExpression childCron = new CronExpression(childCronExpression);

        long parentInterval = interval(parentCron);
        long childInterval = interval(childCron);

        long theBiggerInterval = parentInterval > childInterval ? parentInterval : childInterval;

        ChronoUnit truncateUnit = truncateUnit(theBiggerInterval);
        Integer cycles = numberOfCycles(theBiggerInterval);

        Date baseTime;

        LocalDateTime truncated;
        try {
            truncated = childScheduleTime.truncatedTo(truncateUnit)
                                         .minus(cycles - 1, truncateUnit);
        } catch (UnsupportedTemporalTypeException e) {
            if (childInterval < parentInterval) {
                truncated = previousScheduleTimeOf(parentCronExpression, childScheduleTime)
                        .truncatedTo(DAYS)
                        .minus(cycles - 1, DAYS);
            } else if (childInterval == parentInterval) {
                truncated = childScheduleTime.truncatedTo(DAYS)
                                             .minus(cycles - 1, truncateUnit);
            } else {
                truncated = preScheduleTime(childCronExpression, childScheduleTime)
                        .truncatedTo(DAYS)
                        .minus(cycles - 1, DAYS);
            }
        }

        if (childScheduleTime.equals(truncated)) {
            baseTime = date(childScheduleTime);
        } else {
            baseTime = date(truncated);
        }

        Date parentTime = nextValid(parentCron, baseTime);
        Date preParentTime = preScheduleTime(parentCronExpression, parentTime);

        if (childInterval <= parentInterval) {
            if (parentCron.isSatisfiedBy(baseTime)) {
                result.add(baseTime);
                return result;
            }

            if (parentTime.getTime() - preParentTime.getTime() == parentInterval) {
                result.add(parentTime);
                return result;
            }

            Date childTime = date(childScheduleTime);
            Date preChildTime = preScheduleTime(childCronExpression, childTime);
            Date nextChildTime = nextScheduleTime(childCronExpression, childTime);

            long parentDistance = parentTime.getTime() - preParentTime.getTime();
            long childDistance = childTime.getTime() - preChildTime.getTime();

            if (childDistance == parentDistance && parentTime.after(baseTime) && parentTime.before(nextChildTime)) {
                result.add(parentTime);
                return result;
            }

            result.add(preParentTime);
            return result;
        }

        CronTriggerImpl cronTrigger = new CronTriggerImpl();
        cronTrigger.setCronExpression(parentCronExpression);

        Date baseDateTimePre = new Date(baseTime.getTime() - theBiggerInterval);
        Date parentRelativeBasePre = nextValid(parentCron, baseDateTimePre);

        if (parentTime.getTime() - preParentTime.getTime() != parentInterval) {
            if (parentRelativeBasePre.after(preParentTime)) {
                result.add(preParentTime);
                return result;
            }
            return computeFireTimesBetween(cronTrigger, null, parentRelativeBasePre, preParentTime);
        }

        Date nextParentStart = nextScheduleTime(parentCronExpression, parentRelativeBasePre);

        return computeFireTimesBetween(cronTrigger, null, nextParentStart, parentTime);
    }

    private Date nextValid(CronExpression cron, Date baseTime) {
        if (cron.isSatisfiedBy(baseTime)) {
            return baseTime;
        }
        return cron.getNextValidTimeAfter(baseTime);
    }

    @Override
    public String toString() {
        return "DependencyBuilder{" +
                "parentCronExpression='" + parentCronExpression + '\'' +
                ", childCronExpression='" + childCronExpression + '\'' +
                ", childScheduleTime=" + childScheduleTime +
                ", isParentSelfDepend=" + isParentSelfDepend +
                '}';
    }

    @Getter
    public static class Builder {
        private String parentCronExpression;
        private String childCronExpression;
        private LocalDateTime childScheduleTime;
        private boolean isParentSelfDepend = false;

        public Builder parentCronExpression(String parentCronExpression) {
            this.parentCronExpression = parentCronExpression;
            return this;
        }

        public Builder childCronExpression(String childCronExpression) {
            this.childCronExpression = childCronExpression;
            return this;
        }

        public Builder childScheduleTime(LocalDateTime childScheduleTime) {
            this.childScheduleTime = childScheduleTime;
            return this;
        }

        public Builder isParentSelfDepend(boolean isParentSelfDepend) {
            this.isParentSelfDepend = isParentSelfDepend;
            return this;
        }

        public DefaultDependencyResolver build() {
            return new DefaultDependencyResolver(this);
        }
    }
}
