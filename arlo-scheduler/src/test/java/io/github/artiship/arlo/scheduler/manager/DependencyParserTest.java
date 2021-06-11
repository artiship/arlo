package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.model.dependency.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeInclusiveType.*;
import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeOffsetType.DAY;
import static io.github.artiship.arlo.scheduler.model.dependency.DependencyRangeOffsetType.HOUR;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DependencyParserTest {

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
        DependencyRange currentDay = DependencyRange.of("[d(-1),d(1)]");
        assertThat(currentDay.getStartInclusiveType()).isEqualTo(INCLUSIVE);
        assertThat(currentDay.getStartOffsetType()).isEqualTo(DAY);
        assertThat(currentDay.getStartOffset()).isEqualTo(-1);
        assertThat(currentDay.getEndInclusiveType()).isEqualTo(INCLUSIVE);
        assertThat(currentDay.getEndOffsetType()).isEqualTo(DAY);
        assertThat(currentDay.getEndOffset()).isEqualTo(1);

        DependencyRange lastHour = DependencyRange.of("(h(1))");
        assertThat(lastHour.getStartInclusiveType()).isEqualTo(NON_INCLUSIVE);
        assertThat(lastHour.getStartOffsetType()).isEqualTo(HOUR);
        assertThat(lastHour.getStartOffset()).isEqualTo(1);
        assertThat(lastHour.getEndInclusiveType()).isEqualTo(NON_INCLUSIVE);
        assertThat(lastHour.getEndOffsetType()).isEqualTo(null);
        assertThat(lastHour.getEndOffset()).isEqualTo(null);
    }
}
