package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.model.dependency.DependencyRule;
import io.github.artiship.arlo.scheduler.model.dependency.DependencyRuleType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

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
}
