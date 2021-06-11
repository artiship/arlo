package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.Data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class DependencyRange {
    private DependencyRangeInclusiveType startInclusiveType;
    private DependencyRangeOffsetType startOffsetType;
    private Integer startOffset;
    private DependencyRangeInclusiveType endInclusiveType;
    private DependencyRangeOffsetType endOffsetType;
    private Integer endOffset;

    public static final String DEPENDENCY_RANGE_REGEX
            = "(\\(|\\[)((s|m|h|d|w|M|y)\\((-?[1-9]\\d{0,2})\\)),?((s|m|h|d|w|M|y)\\((-?[1-9]\\d{0,2})\\))?(\\)|\\])";

    private static Pattern pattern = Pattern.compile(DEPENDENCY_RANGE_REGEX);

    public static DependencyRange of(String rangeExpression) {
        final Matcher matcher = pattern.matcher(rangeExpression);

        if (matcher.find()) {
            DependencyRange dependencyRange = new DependencyRange();
            dependencyRange.setStartInclusiveType(DependencyRangeInclusiveType.of(matcher.group(1)));
            dependencyRange.setStartOffsetType(DependencyRangeOffsetType.of(matcher.group(3)));
            dependencyRange.setStartOffset(Integer.parseInt(matcher.group(4)));

            dependencyRange.setEndInclusiveType(DependencyRangeInclusiveType.of(matcher.group(8)));
            if (matcher.group(6) != null) {
                dependencyRange.setEndOffsetType(DependencyRangeOffsetType.of(matcher.group(6)));
            }
            if (matcher.group(7) != null) {
                dependencyRange.setEndOffset(Integer.parseInt(matcher.group(7)));
            }
            return dependencyRange;
        }

        throw new IllegalArgumentException("Dependency rule expression is not supported: " + rangeExpression);
    }
}
