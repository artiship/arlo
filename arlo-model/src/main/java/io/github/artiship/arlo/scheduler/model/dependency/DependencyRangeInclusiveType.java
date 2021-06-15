package io.github.artiship.arlo.scheduler.model.dependency;

import lombok.Getter;

@Getter
public enum DependencyRangeInclusiveType {
    LEFT_INCLUSIVE("["),
    RIGHT_INCLUSIVE("]"),
    LEFT_NON_INCLUSIVE("("),
    RIGHT_NON_INCLUSIVE(")");

    String typeSymbol;

    DependencyRangeInclusiveType(String typeSymbol) {
        this.typeSymbol = typeSymbol;
    }

    public static DependencyRangeInclusiveType of(String typeSymbol) {
        for (DependencyRangeInclusiveType inclusiveType : values()) {
            if (inclusiveType.typeSymbol.equals(typeSymbol)) {
                return inclusiveType;
            }
        }
        throw new IllegalArgumentException("Unsupported dependency range offset type symbol " + typeSymbol);
    }
}
