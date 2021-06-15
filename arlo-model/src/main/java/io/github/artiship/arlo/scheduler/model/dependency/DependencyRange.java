package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class DependencyRange {
    private DependencyRangeInclusiveType startInclusiveType;
    private DependencyRangeInclusiveType endInclusiveType;
    private List<DependencyRangeOffset> startOffsets;
    private List<DependencyRangeOffset> endOffsets;

    private static final String START_OFFSETS_KEY = "startOffsets";
    private static final String END_OFFSETS_KEY = "endOffsets";

    public static final String DEPENDENCY_RANGE_REGEX = "^(\\(|\\[)(.*)(\\)|\\])$";

    private static Pattern rangePattern = Pattern.compile(DEPENDENCY_RANGE_REGEX);

    public static DependencyRange of(String rangeExpression) {
        final Matcher rangeInclusiveMatcher = rangePattern.matcher(rangeExpression);

        if (rangeInclusiveMatcher.find()) {
            DependencyRange dependencyRange = new DependencyRange();
            dependencyRange.setStartInclusiveType(DependencyRangeInclusiveType.of(rangeInclusiveMatcher.group(1)));
            dependencyRange.setEndInclusiveType(DependencyRangeInclusiveType.of(rangeInclusiveMatcher.group(3)));

            String rangeOffsets = rangeInclusiveMatcher.group(2);

            if (rangeOffsets == null || rangeOffsets.length() == 0) {
                throw new IllegalArgumentException("Dependency range expression is invalid: " + rangeExpression);
            }

            if (rangeOffsets.indexOf(",") > 0) {
                String[] splits = rangeInclusiveMatcher.group(2)
                                                       .split(",");

                dependencyRange.setStartOffsets(DependencyRangeOffset.offsets(splits[0]));
                dependencyRange.setEndOffsets(DependencyRangeOffset.offsets(splits[1]));
                return dependencyRange;
            }

            dependencyRange.setStartOffsets(DependencyRangeOffset.offsets(rangeOffsets));
            dependencyRange.setEndOffsets(Collections.emptyList());
            return dependencyRange;
        }

        throw new IllegalArgumentException("Dependency rule expression is not supported: " + rangeExpression);
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer(this.startInclusiveType.toString());
        for(DependencyRangeOffset offset : this.getStartOffsets()) {
            buffer.append(offset.toString());
        }

        buffer.append(",");

        for(DependencyRangeOffset offset: this.getEndOffsets()) {
            buffer.append(offset.toString());
        }

        buffer.append(this.endInclusiveType.toString());
        return buffer.toString();
    }
}
