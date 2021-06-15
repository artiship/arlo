package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;

@Data
@AllArgsConstructor
public class DependencyRangeOffset {
    private DependencyRangeOffsetType offsetType;
    private Integer offset;

    public static final String DEPENDENCY_RANGE_OFFSET_REGEX = "(s|m|h|d|w|M|y)\\((-?[1-9]\\d{0,2})\\)(,)?";
    private static Pattern rangePattern = Pattern.compile(DEPENDENCY_RANGE_OFFSET_REGEX);

    public static DependencyRangeOffset of(DependencyRangeOffsetType offsetType, Integer offset) {
        return new DependencyRangeOffset(offsetType, offset);
    }

    public static List<DependencyRangeOffset> offsets(String rangeOffset) {
        List<DependencyRangeOffset> startOffsets = new ArrayList<>();

        final Matcher rangeMatcher = rangePattern.matcher(rangeOffset);
        while ((rangeMatcher.find())) {
            startOffsets.add(DependencyRangeOffset.of(DependencyRangeOffsetType.of(rangeMatcher.group(1)),
                    parseInt(rangeMatcher.group(2))));
        }

        return startOffsets;
    }

    @Override
    public String toString() {
        return this.offsetType.toString() + "(" + this.offset + ")";
    }
}
