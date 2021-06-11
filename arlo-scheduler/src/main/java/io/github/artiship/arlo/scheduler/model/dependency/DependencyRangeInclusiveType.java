package io.github.artiship.arlo.scheduler.model.dependency;

public enum DependencyRangeInclusiveType {
    INCLUSIVE, NON_INCLUSIVE;

    public static DependencyRangeInclusiveType of(String typeSymbol) {
        if ("[".equals(typeSymbol) || "]".equals(typeSymbol))
            return INCLUSIVE;

        if ("(".equals(typeSymbol) || ")".equals(typeSymbol))
            return NON_INCLUSIVE;

        throw new IllegalArgumentException("Unsupported dependency range inclusive type symbol " + typeSymbol);
    }
}
