package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.Data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class DependencyRule {
    private DependencyRuleType ruleType;
    private Integer scalar;

    public static final String DEPENDENCY_RULE_REGEX = "(\\*|A|H|L|C)(\\((\\d)\\))?";

    private static Pattern pattern = Pattern.compile(DEPENDENCY_RULE_REGEX);

    public static DependencyRule of(String ruleExpression) {
        final Matcher matcher = pattern.matcher(ruleExpression);

        if (matcher.find()) {
            DependencyRule dependencyRule = new DependencyRule();
            dependencyRule.setRuleType(DependencyRuleType.of(matcher.group(1)));
            if (matcher.group(3) != null) {
                dependencyRule.setScalar(Integer.parseInt(matcher.group(3)));
            }

            return dependencyRule;
        }

        throw new IllegalArgumentException("Dependency rule expression is not supported: " + ruleExpression);
    }

    @Override
    public String toString() {
        if (ruleType == DependencyRuleType.ALL)
            return ruleType.getTypeSymbol();

        return ruleType.getTypeSymbol() + "(" + scalar + ")";
    }
}
