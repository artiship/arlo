package io.github.artiship.arlo.scheduler.model.dependency;

public enum DependencyRangeOffsetType {
    SECOND("s"),
    MINUTE("m"),
    HOUR("h"),
    DAY("d"),
    WEEK("w"),
    MONTH("M"),
    YEAR("y");

    String typeSymbol;

    DependencyRangeOffsetType(String typeSymbol) {
        this.typeSymbol = typeSymbol;
    }

    public static DependencyRangeOffsetType of(String typeSymbol) {
        for (DependencyRangeOffsetType offsetType : values()) {
            if (offsetType.typeSymbol.equals(typeSymbol)) {
                return offsetType;
            }
        }
        throw new IllegalArgumentException("Unsupported dependency range offset type symbol " + typeSymbol);
    }
}
