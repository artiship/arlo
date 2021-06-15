package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.model.dependency.DependencyRange;
import io.github.artiship.arlo.scheduler.model.dependency.DependencyRule;
import io.github.artiship.arlo.scheduler.model.dependency.DependencyRuleType;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeInclusiveType.*;
import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeOffset.of;
import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeOffsetType.DAY;
import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeOffsetType.HOUR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CustomizeDependencyResolverTest {

    @Test
    public void parse_dependency_rule() {
        DependencyRule all = DependencyRule.of("*");
        assertThat(all.getRuleType()).isEqualTo(DependencyRuleType.ALL);

        DependencyRule any = DependencyRule.of("A");
        assertThat(any.getRuleType()).isEqualTo(DependencyRuleType.ANY);

        DependencyRule headOf = DependencyRule.of("H(1)");
        assertThat(headOf.getRuleType()).isEqualTo(DependencyRuleType.HEAD_OF);
        assertThat(headOf.getScalar()).isEqualTo(1);

        DependencyRule lastOf = DependencyRule.of("L(2)");
        assertThat(lastOf.getRuleType()).isEqualTo(DependencyRuleType.LAST_OF);
        assertThat(lastOf.getScalar()).isEqualTo(2);

        DependencyRule continuousOf = DependencyRule.of("C(3)");
        assertThat(continuousOf.getRuleType()).isEqualTo(DependencyRuleType.CONTINUOUS_OF);
        assertThat(continuousOf.getScalar()).isEqualTo(3);
    }

    @Test
    public void parse_dependency_range() {
        DependencyRange range1 = DependencyRange.of("[d(-1),d(1)]");
        assertThat(range1.getStartInclusiveType()).isEqualTo(LEFT_INCLUSIVE);
        assertThat(range1.getEndInclusiveType()).isEqualTo(RIGHT_INCLUSIVE);
        assertThat(range1.getStartOffsets()).isEqualTo(asList(of(DAY, -1)));
        assertThat(range1.getEndOffsets()).isEqualTo(asList(of(DAY, 1)));

        DependencyRange range2 = DependencyRange.of("(h(1))");
        assertThat(range2.getStartInclusiveType()).isEqualTo(LEFT_NON_INCLUSIVE);
        assertThat(range2.getEndInclusiveType()).isEqualTo(RIGHT_NON_INCLUSIVE);
        assertThat(range2.getStartOffsets()).isEqualTo(asList(of(HOUR, 1)));
        assertThat(range2.getEndOffsets()).isEqualTo(Lists.emptyList());

        DependencyRange range3 = DependencyRange.of("[d(-1)h(2),d(1))");
        assertThat(range3.getStartInclusiveType()).isEqualTo(LEFT_INCLUSIVE);
        assertThat(range3.getEndInclusiveType()).isEqualTo(RIGHT_NON_INCLUSIVE);
        assertThat(range3.getStartOffsets()).isEqualTo(asList(of(DAY, -1), of(HOUR, 2)));
        assertThat(range3.getEndOffsets()).isEqualTo(asList(of(DAY, 1)));
    }
}
