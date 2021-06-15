package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.Getter;

@Getter
public enum DependencyRuleType {
    ALL("*"),
    ANY("A"),
    HEAD_OF("H"),
    LAST_OF("L"),
    CONTINUOUS_OF("C");

    String typeSymbol;

    DependencyRuleType(String typeSymbol) {
        this.typeSymbol = typeSymbol;
    }

    public static DependencyRuleType of(String typeSymbol) {
        for (DependencyRuleType ruleType : values()) {
            if (ruleType.typeSymbol.equals(typeSymbol)) {
                return ruleType;
            }
        }
        throw new IllegalArgumentException("unsupported dependency rule type symbol " + typeSymbol);
    }


}
